/*
 * Copyright 2025 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.tools.nrt_utils.backup;

import static com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.IncrementalCommandUtils.fromUTF8;
import static com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.IncrementalCommandUtils.toUTF8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.CompressingInputStream;
import com.yelp.nrtsearch.server.remote.FileCompressor;
import com.yelp.nrtsearch.server.remote.LZ4FileCompressor;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.remote.ZstdFileCompressor;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import picocli.CommandLine;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.BlockingInputStreamAsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;

/**
 * Command to copy an existing snapshot to a new snapshot root, compressing all index data files
 * with zstd or lz4. Compression statistics are reported for each file, and in aggregate. The output
 * is a complete snapshot, with the point state updated to record the compression of each file, so
 * it may be restored with the {@link RestoreCommand}.
 */
@CommandLine.Command(
    name = CompressSnapshotCommand.COMPRESS_SNAPSHOT,
    description =
        "Copy snapshot of index data to a new snapshot root, compressing all index files with zstd or lz4.")
public class CompressSnapshotCommand implements Callable<Integer> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final String COMPRESS_SNAPSHOT = "compressSnapshot";

  /** Compression types supported by this command. */
  enum CompressionType {
    ZSTD,
    LZ4
  }

  @CommandLine.Option(
      names = {"-b", "--bucketName"},
      description = "Name of bucket containing index files",
      required = true)
  private String bucketName;

  @CommandLine.Option(
      names = {"--region"},
      description = "AWS region name, such as us-west-1, us-west-2, us-east-1")
  private String region;

  @CommandLine.Option(
      names = {"-c", "--credsFile"},
      description =
          "File holding AWS credentials; Will use DefaultCredentialProvider if this is unset.")
  private String credsFile;

  @CommandLine.Option(
      names = {"-p", "--credsProfile"},
      description = "Profile to use from creds file; Neglected when credsFile is unset.",
      defaultValue = "default")
  private String credsProfile;

  @CommandLine.Option(
      names = {"--snapshotIndexIdentifier"},
      description = "Index identifier for snapshot, in the form <index_name>-<time_string>",
      required = true)
  private String snapshotIndexIdentifier;

  @CommandLine.Option(
      names = {"--snapshotTimeString"},
      description =
          "Time string of snapshot data to compress, must be of the form yyyyMMddHHmmssSSS",
      required = true)
  private String snapshotTimeString;

  @CommandLine.Option(
      names = {"--snapshotRoot"},
      description =
          "Root s3 snapshot path, defaults to <snapshotServiceName>/snapshots/ either this or snapshotServiceName must be specified")
  private String snapshotRoot;

  @CommandLine.Option(
      names = {"--snapshotServiceName"},
      description =
          "Name of nrtsearch cluster for snapshot, either this or snapshotRoot must be specified")
  private String snapshotServiceName;

  @CommandLine.Option(
      names = {"--destSnapshotRoot"},
      description =
          "Root s3 snapshot path to write the compressed snapshot to, must differ from the source snapshot root",
      required = true)
  private String destSnapshotRoot;

  @CommandLine.Option(
      names = {"--compressionType"},
      description =
          "Compression to apply to index files, one of: ${COMPLETION-CANDIDATES}, (default: ${DEFAULT-VALUE})",
      defaultValue = "ZSTD")
  CompressionType compressionType;

  @CommandLine.Option(
      names = {"--compressionLevel"},
      description =
          "Zstd compression level, ignored for LZ4, negative values select the fast levels, (default: ${DEFAULT-VALUE})",
      defaultValue = "" + ZstdFileCompressor.DEFAULT_LEVEL)
  int compressionLevel;

  @CommandLine.Option(
      names = {"--compressionWorkers"},
      description =
          "Number of zstd worker threads used to compress each file, 0 for single-threaded, ignored for LZ4, (default: ${DEFAULT-VALUE})",
      defaultValue = "" + ZstdFileCompressor.DEFAULT_WORKERS)
  int compressionWorkers;

  @CommandLine.Option(
      names = {"--fileConcurrency"},
      description = "Number of files to compress concurrently, (default: ${DEFAULT-VALUE})",
      defaultValue = "4")
  int fileConcurrency;

  @CommandLine.Option(
      names = {"--inMemoryThresholdBytes"},
      description =
          "Files at or below this size are compressed into memory before upload, larger files are streamed with a multipart upload, (default: ${DEFAULT-VALUE})",
      defaultValue = "" + S3Backend.S3BackendConfig.DEFAULT_COMPRESSION_IN_MEMORY_THRESHOLD_BYTES)
  long inMemoryThresholdBytes;

  @CommandLine.Option(
      names = {"--maxConcurrency"},
      description = "Maximum connection to make to S3, (default: ${DEFAULT-VALUE})",
      defaultValue = "100")
  int maxConcurrency;

  @CommandLine.Option(
      names = {"--connectionAcquisitionTimeoutMs"},
      description =
          "Connection acquisition timeout in milliseconds for async S3 client, (default: ${DEFAULT-VALUE})",
      defaultValue = "60000")
  long connectionAcquisitionTimeoutMs;

  @CommandLine.Option(
      names = {"--maxRetry"},
      description = "Maximum number of retry attempts for S3 failed requests",
      defaultValue = "20")
  private int maxRetry;

  private S3Util.S3ClientBundle s3ClientBundle;

  @VisibleForTesting
  void setS3ClientBundle(S3Util.S3ClientBundle s3ClientBundle) {
    this.s3ClientBundle = s3ClientBundle;
  }

  /** Compression result for a single index file. */
  record FileResult(
      String fileName,
      long uncompressedBytes,
      long compressedBytes,
      long compressNanos,
      long totalNanos) {}

  @Override
  public Integer call() throws Exception {
    FileCompressor compressor = createCompressor();
    if (fileConcurrency < 1) {
      throw new IllegalArgumentException("fileConcurrency must be >= 1");
    }
    if (s3ClientBundle == null) {
      s3ClientBundle =
          StateCommandUtils.createS3ClientBundle(
              bucketName,
              region,
              credsFile,
              credsProfile,
              maxRetry,
              maxConcurrency,
              connectionAcquisitionTimeoutMs);
    }
    S3Client s3Client = s3ClientBundle.s3Client();

    String resolvedSnapshotRoot =
        BackupCommandUtils.getSnapshotRoot(snapshotRoot, snapshotServiceName);
    String resolvedDestRoot = BackupCommandUtils.getSnapshotRoot(destSnapshotRoot, null);
    if (resolvedSnapshotRoot.equals(resolvedDestRoot)) {
      throw new IllegalArgumentException(
          "destSnapshotRoot must differ from the source snapshot root: " + resolvedSnapshotRoot);
    }
    String sourceDataRoot =
        BackupCommandUtils.getSnapshotIndexDataRoot(
            resolvedSnapshotRoot, snapshotIndexIdentifier, snapshotTimeString);
    String destDataRoot =
        BackupCommandUtils.getSnapshotIndexDataRoot(
            resolvedDestRoot, snapshotIndexIdentifier, snapshotTimeString);
    String sourceMetadataKey =
        BackupCommandUtils.getSnapshotIndexMetadataKey(
            resolvedSnapshotRoot, snapshotIndexIdentifier, snapshotTimeString);
    String destMetadataKey =
        BackupCommandUtils.getSnapshotIndexMetadataKey(
            resolvedDestRoot, snapshotIndexIdentifier, snapshotTimeString);

    if (!S3Util.doesKeyExist(s3Client, bucketName, sourceMetadataKey)) {
      throw new IllegalArgumentException("Metadata file does not exist: " + sourceMetadataKey);
    }
    checkDestNotExists(s3Client, destDataRoot, destMetadataKey);

    System.out.println(
        "Starting compression of snapshot: "
            + sourceDataRoot
            + ", dest: "
            + destDataRoot
            + ", "
            + describeCompression());

    SnapshotMetadata sourceMetadata = loadMetadata(s3Client, sourceMetadataKey);
    NrtPointState pointState =
        RemoteUtils.pointStateFromUtf8(
            readObject(s3Client, sourceDataRoot + BackupCommandUtils.SNAPSHOT_POINT_STATE));

    long startNanos = System.nanoTime();
    List<FileResult> results =
        compressIndexFiles(compressor, pointState, sourceDataRoot, destDataRoot);
    long wallNanos = System.nanoTime() - startNanos;

    long totalCompressed = results.stream().mapToLong(FileResult::compressedBytes).sum();
    putObject(
        s3Client,
        destDataRoot + BackupCommandUtils.SNAPSHOT_POINT_STATE,
        RemoteUtils.pointStateToUtf8(pointState));
    copyObject(
        s3Client,
        sourceDataRoot + BackupCommandUtils.SNAPSHOT_INDEX_STATE,
        destDataRoot + BackupCommandUtils.SNAPSHOT_INDEX_STATE);
    String sourceWarmingQueriesKey = sourceDataRoot + BackupCommandUtils.SNAPSHOT_WARMING_QUERIES;
    if (S3Util.doesKeyExist(s3Client, bucketName, sourceWarmingQueriesKey)) {
      copyObject(
          s3Client,
          sourceWarmingQueriesKey,
          destDataRoot + BackupCommandUtils.SNAPSHOT_WARMING_QUERIES);
    } else {
      System.out.println("Warming queries not present in snapshot, skipping");
    }
    // written last, so the compressed snapshot is only visible once complete
    SnapshotMetadata destMetadata =
        new SnapshotMetadata(
            sourceMetadata.getServiceName(),
            sourceMetadata.getIndexName(),
            sourceMetadata.getTimeStringMs(),
            totalCompressed);
    putObject(s3Client, destMetadataKey, toUTF8(OBJECT_MAPPER.writeValueAsString(destMetadata)));

    printSummary(results, wallNanos);
    System.out.println("Compress snapshot completed");
    return 0;
  }

  private List<FileResult> compressIndexFiles(
      FileCompressor compressor,
      NrtPointState pointState,
      String sourceDataRoot,
      String destDataRoot)
      throws InterruptedException, IOException {
    S3TransferManager transferManager =
        S3TransferManager.builder().s3Client(s3ClientBundle.s3AsyncClient()).build();
    ExecutorService executor = Executors.newFixedThreadPool(fileConcurrency);
    try {
      List<Future<FileResult>> futures = new ArrayList<>();
      for (Map.Entry<String, NrtFileMetaData> entry : pointState.files.entrySet()) {
        futures.add(
            executor.submit(
                () ->
                    compressIndexFile(
                        compressor,
                        transferManager,
                        entry.getKey(),
                        entry.getValue(),
                        sourceDataRoot,
                        destDataRoot)));
      }
      List<FileResult> results = new ArrayList<>();
      for (Future<FileResult> future : futures) {
        try {
          results.add(future.get());
        } catch (ExecutionException e) {
          throw new IOException("Error compressing index file", e.getCause());
        }
      }
      return results;
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(1, TimeUnit.MINUTES);
      transferManager.close();
    }
  }

  private FileResult compressIndexFile(
      FileCompressor compressor,
      S3TransferManager transferManager,
      String fileName,
      NrtFileMetaData meta,
      String sourceDataRoot,
      String destDataRoot)
      throws IOException {
    String backendFileName = S3Backend.getIndexBackendFileName(fileName, meta);
    String sourceKey = sourceDataRoot + backendFileName;
    String destKey = destDataRoot + backendFileName;
    TimingFileCompressor timingCompressor = new TimingFileCompressor(compressor);

    long startNanos = System.nanoTime();
    long compressedBytes;
    try (InputStream source = openSourceFile(sourceKey, meta)) {
      if (meta.length <= inMemoryThresholdBytes) {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (OutputStream compressingStream = timingCompressor.compressStream(compressed)) {
          IOUtils.copy(source, compressingStream);
        }
        putObject(s3ClientBundle.s3Client(), destKey, compressed.toByteArray());
        compressedBytes = compressed.size();
      } else {
        compressedBytes = streamingUpload(transferManager, destKey, source, timingCompressor);
      }
    }
    long totalNanos = System.nanoTime() - startNanos;

    if (timingCompressor.uncompressedBytes != meta.length) {
      throw new IllegalStateException(
          "Index file "
              + backendFileName
              + " size mismatch, expected: "
              + meta.length
              + ", got: "
              + timingCompressor.uncompressedBytes);
    }
    meta.compressionType = compressionType.name();
    meta.compressedLength = compressedBytes;

    FileResult result =
        new FileResult(
            backendFileName,
            meta.length,
            compressedBytes,
            timingCompressor.compressNanos,
            totalNanos);
    System.out.println(
        String.format(
            "Compressed %s: %d -> %d bytes, ratio: %.3f, compress time: %d ms, total time: %d ms",
            backendFileName,
            result.uncompressedBytes(),
            result.compressedBytes(),
            ratio(result.uncompressedBytes(), result.compressedBytes()),
            TimeUnit.NANOSECONDS.toMillis(result.compressNanos()),
            TimeUnit.NANOSECONDS.toMillis(result.totalNanos())));
    return result;
  }

  /** Open a stream of the uncompressed source file, decompressing it if needed. */
  private InputStream openSourceFile(String sourceKey, NrtFileMetaData meta) throws IOException {
    InputStream source =
        s3ClientBundle
            .s3Client()
            .getObject(GetObjectRequest.builder().bucket(bucketName).key(sourceKey).build());
    if (meta.compressionType == null) {
      return source;
    }
    FileCompressor sourceCompressor =
        switch (meta.compressionType) {
          case "LZ4" -> new LZ4FileCompressor();
          case "ZSTD" ->
              new ZstdFileCompressor(
                  ZstdFileCompressor.DEFAULT_LEVEL, ZstdFileCompressor.DEFAULT_WORKERS);
          default -> {
            source.close();
            throw new IllegalArgumentException(
                "Unsupported source compression type: " + meta.compressionType);
          }
        };
    return sourceCompressor.decompressStream(source);
  }

  /**
   * Upload compressed data of unknown length, letting the multipart client split it into parts.
   * Compression runs on the calling thread as the request body is written.
   */
  private long streamingUpload(
      S3TransferManager transferManager,
      String destKey,
      InputStream source,
      FileCompressor compressor)
      throws IOException {
    BlockingInputStreamAsyncRequestBody body = AsyncRequestBody.forBlockingInputStream(null);
    Upload upload =
        transferManager.upload(
            UploadRequest.builder()
                .putObjectRequest(
                    PutObjectRequest.builder().bucket(bucketName).key(destKey).build())
                .requestBody(body)
                .build());
    // If S3 fails before consuming the whole body, release the writer blocked on it.
    upload
        .completionFuture()
        .whenComplete(
            (r, t) -> {
              if (t != null) {
                body.cancel();
              }
            });
    long compressedBytes;
    try (CompressingInputStream compressedStream = new CompressingInputStream(source, compressor)) {
      body.writeInputStream(compressedStream);
      compressedBytes = compressedStream.getCompressedBytes();
    } catch (Throwable t) {
      body.cancel();
      throw t;
    }
    upload.completionFuture().join();
    return compressedBytes;
  }

  private FileCompressor createCompressor() {
    return switch (compressionType) {
      case ZSTD -> new ZstdFileCompressor(compressionLevel, compressionWorkers);
      case LZ4 -> new LZ4FileCompressor();
    };
  }

  private String describeCompression() {
    return switch (compressionType) {
      case ZSTD ->
          "compression: ZSTD, level: " + compressionLevel + ", workers: " + compressionWorkers;
      case LZ4 -> "compression: LZ4";
    };
  }

  private void printSummary(List<FileResult> results, long wallNanos) {
    long totalUncompressed = results.stream().mapToLong(FileResult::uncompressedBytes).sum();
    long totalCompressed = results.stream().mapToLong(FileResult::compressedBytes).sum();
    long totalCompressNanos = results.stream().mapToLong(FileResult::compressNanos).sum();
    double compressSeconds = totalCompressNanos / 1e9;
    double wallSeconds = wallNanos / 1e9;
    System.out.println("Compression summary:");
    System.out.println("  " + describeCompression());
    System.out.println("  files: " + results.size());
    System.out.println("  uncompressed bytes: " + totalUncompressed);
    System.out.println("  compressed bytes: " + totalCompressed);
    System.out.println(
        String.format(
            "  ratio: %.3f, space saved: %.1f%%",
            ratio(totalUncompressed, totalCompressed),
            totalUncompressed == 0
                ? 0.0
                : 100.0 * (totalUncompressed - totalCompressed) / totalUncompressed));
    System.out.println(
        String.format(
            "  compress time (summed over files): %.3f s, %.1f MB/s per thread",
            compressSeconds,
            compressSeconds == 0 ? 0.0 : totalUncompressed / compressSeconds / (1024 * 1024)));
    System.out.println(
        String.format(
            "  wall time: %.3f s, %.1f MB/s overall",
            wallSeconds, wallSeconds == 0 ? 0.0 : totalUncompressed / wallSeconds / (1024 * 1024)));
  }

  private static double ratio(long uncompressed, long compressed) {
    return compressed == 0 ? 0.0 : (double) uncompressed / compressed;
  }

  private void checkDestNotExists(S3Client s3Client, String destDataRoot, String destMetadataKey) {
    if (S3Util.doesKeyExist(s3Client, bucketName, destMetadataKey)) {
      throw new IllegalArgumentException("Destination metadata exists: " + destMetadataKey);
    }
    if (!s3Client
        .listObjectsV2(
            ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(destDataRoot)
                .maxKeys(1)
                .build())
        .contents()
        .isEmpty()) {
      throw new IllegalArgumentException("Data exists at destination location: " + destDataRoot);
    }
  }

  private SnapshotMetadata loadMetadata(S3Client s3Client, String metadataKey) throws IOException {
    System.out.println("Loading metadata key: " + metadataKey);
    return OBJECT_MAPPER.readValue(
        fromUTF8(readObject(s3Client, metadataKey)), SnapshotMetadata.class);
  }

  private byte[] readObject(S3Client s3Client, String key) throws IOException {
    try (InputStream input =
        s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build())) {
      return IOUtils.toByteArray(input);
    }
  }

  private void putObject(S3Client s3Client, String key, byte[] data) {
    PutObjectRequest putRequest =
        PutObjectRequest.builder()
            .bucket(bucketName)
            .key(key)
            .contentLength((long) data.length)
            .build();
    s3Client.putObject(putRequest, RequestBody.fromBytes(data));
  }

  private void copyObject(S3Client s3Client, String sourceKey, String destKey) {
    System.out.println("Copying " + sourceKey + " to " + destKey);
    s3Client.copyObject(
        CopyObjectRequest.builder()
            .sourceBucket(bucketName)
            .sourceKey(sourceKey)
            .destinationBucket(bucketName)
            .destinationKey(destKey)
            .build());
  }

  /**
   * Compressor wrapper that records the time spent in, and the number of bytes written to, the
   * compressing stream. Each instance is used for a single file, by a single thread.
   */
  private static class TimingFileCompressor implements FileCompressor {
    private final FileCompressor delegate;
    long compressNanos = 0;
    long uncompressedBytes = 0;

    TimingFileCompressor(FileCompressor delegate) {
      this.delegate = delegate;
    }

    @Override
    public OutputStream compressStream(OutputStream output) throws IOException {
      return new FilterOutputStream(delegate.compressStream(output)) {
        @Override
        public void write(int b) throws IOException {
          long start = System.nanoTime();
          out.write(b);
          compressNanos += System.nanoTime() - start;
          uncompressedBytes++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          long start = System.nanoTime();
          out.write(b, off, len);
          compressNanos += System.nanoTime() - start;
          uncompressedBytes += len;
        }

        @Override
        public void flush() throws IOException {
          long start = System.nanoTime();
          out.flush();
          compressNanos += System.nanoTime() - start;
        }

        @Override
        public void close() throws IOException {
          long start = System.nanoTime();
          out.close();
          compressNanos += System.nanoTime() - start;
        }
      };
    }

    @Override
    public InputStream decompressStream(InputStream compressed) throws IOException {
      return delegate.decompressStream(compressed);
    }
  }
}
