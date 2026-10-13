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

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import org.apache.commons.io.IOUtils;
import picocli.CommandLine;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Command to measure decompression of the index files in a snapshot, such as one written by {@link
 * CompressSnapshotCommand}. Each file is downloaded in full before it is decompressed, so the
 * reported decompression time does not include S3 transfer. Decompressed data is verified against
 * the file length and checksum in the snapshot point state, then discarded; nothing is written to
 * S3.
 */
@CommandLine.Command(
    name = DecompressSnapshotCommand.DECOMPRESS_SNAPSHOT,
    description =
        "Download and decompress the index files of a snapshot, verifying them and reporting decompression statistics. Nothing is written to S3.")
public class DecompressSnapshotCommand implements Callable<Integer> {
  public static final String DECOMPRESS_SNAPSHOT = "decompressSnapshot";
  private static final int BUFFER_SIZE = 64 * 1024;
  // lucene index files end with a checksum of all preceding bytes
  private static final int CHECKSUM_LENGTH = Long.BYTES;

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
          "Time string of snapshot data to decompress, must be of the form yyyyMMddHHmmssSSS",
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
      names = {"--fileConcurrency"},
      description =
          "Number of files to download and decompress concurrently, (default: ${DEFAULT-VALUE})",
      defaultValue = "4")
  int fileConcurrency;

  @CommandLine.Option(
      names = {"--inMemoryThresholdBytes"},
      description =
          "Files stored in S3 at or below this size are downloaded into memory, larger files are downloaded to a temporary file, (default: ${DEFAULT-VALUE})",
      defaultValue = "" + S3Backend.S3BackendConfig.DEFAULT_COMPRESSION_IN_MEMORY_THRESHOLD_BYTES)
  long inMemoryThresholdBytes;

  @CommandLine.Option(
      names = {"--tempDir"},
      description =
          "Directory for temporary files holding large downloads, defaults to the system temporary directory")
  private String tempDir;

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

  /** Decompression result for a single index file. */
  record FileResult(
      String fileName,
      String luceneFileName,
      String compressionType,
      long storedBytes,
      long uncompressedBytes,
      long decompressNanos,
      long downloadNanos) {}

  @Override
  public Integer call() throws Exception {
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
    String dataRoot =
        BackupCommandUtils.getSnapshotIndexDataRoot(
            resolvedSnapshotRoot, snapshotIndexIdentifier, snapshotTimeString);
    String metadataKey =
        BackupCommandUtils.getSnapshotIndexMetadataKey(
            resolvedSnapshotRoot, snapshotIndexIdentifier, snapshotTimeString);
    if (!S3Util.doesKeyExist(s3Client, bucketName, metadataKey)) {
      throw new IllegalArgumentException("Metadata file does not exist: " + metadataKey);
    }

    System.out.println("Starting decompression of snapshot: " + dataRoot);
    NrtPointState pointState =
        RemoteUtils.pointStateFromUtf8(
            readObject(s3Client, dataRoot + BackupCommandUtils.SNAPSHOT_POINT_STATE));

    long startNanos = System.nanoTime();
    List<FileResult> results = decompressIndexFiles(pointState, dataRoot);
    long wallNanos = System.nanoTime() - startNanos;

    printSummary(results, wallNanos);
    System.out.println("Decompress snapshot completed");
    return 0;
  }

  private List<FileResult> decompressIndexFiles(NrtPointState pointState, String dataRoot)
      throws InterruptedException, IOException {
    ExecutorService executor = Executors.newFixedThreadPool(fileConcurrency);
    try {
      List<Future<FileResult>> futures = new ArrayList<>();
      for (Map.Entry<String, NrtFileMetaData> entry : pointState.files.entrySet()) {
        futures.add(
            executor.submit(() -> decompressIndexFile(entry.getKey(), entry.getValue(), dataRoot)));
      }
      List<FileResult> results = new ArrayList<>();
      for (Future<FileResult> future : futures) {
        try {
          results.add(future.get());
        } catch (ExecutionException e) {
          throw new IOException("Error decompressing index file", e.getCause());
        }
      }
      return results;
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(1, TimeUnit.MINUTES);
    }
  }

  private FileResult decompressIndexFile(String fileName, NrtFileMetaData meta, String dataRoot)
      throws IOException {
    String backendFileName = S3Backend.getIndexBackendFileName(fileName, meta);
    String key = dataRoot + backendFileName;
    long storedBytes = meta.compressedLength != null ? meta.compressedLength : meta.length;

    Path tempFile = null;
    try {
      long downloadStart = System.nanoTime();
      InputStream stored;
      if (storedBytes <= inMemoryThresholdBytes) {
        stored = new ByteArrayInputStream(readObject(s3ClientBundle.s3Client(), key));
      } else {
        tempFile =
            tempDir == null
                ? Files.createTempFile("decompress_snapshot", ".tmp")
                : Files.createTempFile(Paths.get(tempDir), "decompress_snapshot", ".tmp");
        try (InputStream input = openObject(s3ClientBundle.s3Client(), key)) {
          Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
        }
        stored = Files.newInputStream(tempFile);
      }
      long downloadNanos = System.nanoTime() - downloadStart;

      long decompressNanos;
      long uncompressedBytes;
      CRC32 checksum = new CRC32();
      try (stored) {
        long start = System.nanoTime();
        InputStream data =
            meta.compressionType == null
                ? stored
                : CompressionCommandUtils.getDecompressor(meta.compressionType)
                    .decompressStream(stored);
        long nanos = System.nanoTime() - start;
        try (data) {
          byte[] buffer = new byte[BUFFER_SIZE];
          long position = 0;
          long checksummedLength = meta.length - CHECKSUM_LENGTH;
          while (true) {
            start = System.nanoTime();
            int read = data.read(buffer);
            nanos += System.nanoTime() - start;
            if (read < 0) {
              break;
            }
            int checksummed = (int) Math.max(0, Math.min(read, checksummedLength - position));
            checksum.update(buffer, 0, checksummed);
            position += read;
          }
          uncompressedBytes = position;
        }
        decompressNanos = meta.compressionType == null ? 0 : nanos;
      }

      if (uncompressedBytes != meta.length) {
        throw new IllegalStateException(
            "Index file "
                + backendFileName
                + " size mismatch, expected: "
                + meta.length
                + ", got: "
                + uncompressedBytes);
      }
      if (checksum.getValue() != meta.checksum) {
        throw new IllegalStateException(
            "Index file "
                + backendFileName
                + " checksum mismatch, expected: "
                + meta.checksum
                + ", got: "
                + checksum.getValue());
      }

      FileResult result =
          new FileResult(
              backendFileName,
              fileName,
              meta.compressionType == null ? "NONE" : meta.compressionType,
              storedBytes,
              uncompressedBytes,
              decompressNanos,
              downloadNanos);
      System.out.println(
          String.format(
              "Decompressed %s (%s): %d -> %d bytes, decompress time: %d ms, download time: %d ms",
              backendFileName,
              result.compressionType(),
              result.storedBytes(),
              result.uncompressedBytes(),
              TimeUnit.NANOSECONDS.toMillis(result.decompressNanos()),
              TimeUnit.NANOSECONDS.toMillis(result.downloadNanos())));
      return result;
    } finally {
      if (tempFile != null) {
        Files.deleteIfExists(tempFile);
      }
    }
  }

  private void printSummary(List<FileResult> results, long wallNanos) {
    Map<String, Integer> typeCounts = new TreeMap<>();
    long totalStored = 0;
    long totalUncompressed = 0;
    long compressedFilesUncompressed = 0;
    long totalDecompressNanos = 0;
    long totalDownloadNanos = 0;
    for (FileResult result : results) {
      typeCounts.merge(result.compressionType(), 1, Integer::sum);
      totalStored += result.storedBytes();
      totalUncompressed += result.uncompressedBytes();
      totalDecompressNanos += result.decompressNanos();
      totalDownloadNanos += result.downloadNanos();
      if (!"NONE".equals(result.compressionType())) {
        compressedFilesUncompressed += result.uncompressedBytes();
      }
    }
    double decompressSeconds = totalDecompressNanos / 1e9;
    double wallSeconds = wallNanos / 1e9;
    System.out.println("Decompression summary:");
    System.out.println("  compression types (files): " + typeCounts);
    System.out.println("  files: " + results.size() + ", all verified");
    System.out.println("  compressed bytes: " + totalStored);
    System.out.println("  uncompressed bytes: " + totalUncompressed);
    System.out.println(
        String.format(
            "  ratio: %.3f, space saved: %.1f%%",
            CompressionCommandUtils.ratio(totalUncompressed, totalStored),
            CompressionCommandUtils.spaceSavedPercent(totalUncompressed, totalStored)));
    System.out.println(
        String.format(
            "  decompress time (summed over files): %.3f s, %.1f MB/s per thread",
            decompressSeconds,
            CompressionCommandUtils.mbPerSecond(
                compressedFilesUncompressed, totalDecompressNanos)));
    System.out.println(
        String.format(
            "  download time (summed over files): %.3f s, %.1f MB/s per thread",
            totalDownloadNanos / 1e9,
            CompressionCommandUtils.mbPerSecond(totalStored, totalDownloadNanos)));
    System.out.println(
        String.format(
            "  wall time: %.3f s, %.1f MB/s overall",
            wallSeconds, CompressionCommandUtils.mbPerSecond(totalUncompressed, wallNanos)));
    CompressionCommandUtils.printFileTypeSummary(
        results.stream()
            .map(
                r ->
                    new CompressionCommandUtils.FileStats(
                        r.luceneFileName(),
                        r.uncompressedBytes(),
                        r.storedBytes(),
                        r.decompressNanos()))
            .toList(),
        "decompress");
  }

  private InputStream openObject(S3Client s3Client, String key) {
    return s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build());
  }

  private byte[] readObject(S3Client s3Client, String key) throws IOException {
    try (InputStream input = openObject(s3Client, key)) {
      return IOUtils.toByteArray(input);
    }
  }
}
