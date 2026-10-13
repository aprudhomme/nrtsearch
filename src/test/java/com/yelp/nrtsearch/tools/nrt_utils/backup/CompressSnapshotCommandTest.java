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

import static com.yelp.nrtsearch.server.grpc.TestServer.S3_ENDPOINT;
import static com.yelp.nrtsearch.server.grpc.TestServer.SERVICE_NAME;
import static com.yelp.nrtsearch.server.grpc.TestServer.TEST_BUCKET;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.FileCompressor;
import com.yelp.nrtsearch.server.remote.FileCompressorCreator;
import com.yelp.nrtsearch.server.remote.LZ4FileCompressor;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.remote.ZstdFileCompressor;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import picocli.CommandLine;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class CompressSnapshotCommandTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String SNAPSHOT_ROOT = SERVICE_NAME + "/" + BackupCommandUtils.SNAPSHOT_DIR;
  private static final String DEST_ROOT = "compressed/snapshots";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void setupClass() {
    // test servers configured with compression need the built-in compressors registered
    FileCompressorCreator.initialize(
        new NrtsearchConfig(new ByteArrayInputStream("nodeName: test".getBytes())), List.of());
  }

  @After
  public void cleanup() {
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  private S3Client getS3() {
    S3Client s3 = AmazonS3Provider.createTestS3Client(S3_ENDPOINT);
    s3.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET).build());
    return s3;
  }

  private S3AsyncClient getS3Async() {
    return AmazonS3Provider.createTestS3AsyncClient(S3_ENDPOINT);
  }

  private TestServer getTestServer(String additionalConfig) throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withRemoteStateBackend(false)
            .withAdditionalConfig(additionalConfig)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    server.addSimpleDocs("test_index", 1, 2);
    server.refresh("test_index");
    server.commit("test_index");
    return server;
  }

  /** Snapshot the test index, returning the snapshot time string. */
  private String snapshot(TestServer server) {
    SnapshotCommand command = new SnapshotCommand();
    command.setS3ClientBundle(new S3Util.S3ClientBundle(getS3(), getS3Async()));
    int exitCode =
        new CommandLine(command)
            .execute(
                "--serviceName=" + SERVICE_NAME,
                "--indexName=test_index",
                "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);
    String metadataPrefix =
        BackupCommandUtils.getSnapshotIndexMetadataPrefix(
            SNAPSHOT_ROOT + "/", getIndexResource(server));
    List<String> keys =
        getS3()
            .listObjectsV2(builder -> builder.bucket(TEST_BUCKET).prefix(metadataPrefix))
            .contents()
            .stream()
            .map(o -> o.key().substring(metadataPrefix.length()))
            .toList();
    assertEquals(1, keys.size());
    return keys.get(0);
  }

  private String getIndexResource(TestServer server) {
    return server.getGlobalState().getDataResourceForIndex("test_index");
  }

  private int runCompress(S3AsyncClient asyncClient, String... args) {
    CompressSnapshotCommand command = new CompressSnapshotCommand();
    command.setS3ClientBundle(new S3Util.S3ClientBundle(getS3(), asyncClient));
    return new CommandLine(command).execute(args);
  }

  private String[] compressArgs(String indexResource, String timeString, String... extra) {
    List<String> args =
        new java.util.ArrayList<>(
            List.of(
                "--bucketName=" + TEST_BUCKET,
                "--snapshotServiceName=" + SERVICE_NAME,
                "--snapshotIndexIdentifier=" + indexResource,
                "--snapshotTimeString=" + timeString,
                "--destSnapshotRoot=" + DEST_ROOT));
    args.addAll(List.of(extra));
    return args.toArray(new String[0]);
  }

  @Test
  public void testCompressSnapshot() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);

    assertEquals(
        0,
        runCompress(getS3Async(), compressArgs(indexResource, timeString, "--compressionLevel=9")));
    assertCompressedSnapshot(indexResource, timeString, null, "ZSTD");
  }

  @Test
  public void testCompressSnapshot_streamingUpload() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);

    AtomicInteger streamingUploads = new AtomicInteger();
    assertEquals(
        0,
        runCompress(
            forwardingAsyncClient(getS3(), streamingUploads),
            compressArgs(
                indexResource,
                timeString,
                "--inMemoryThresholdBytes=0",
                "--compressionWorkers=2",
                "--fileConcurrency=2")));
    NrtPointState pointState = assertCompressedSnapshot(indexResource, timeString, null, "ZSTD");
    assertEquals(pointState.files.size(), streamingUploads.get());
  }

  @Test
  public void testCompressSnapshot_lz4Source() throws IOException {
    TestServer server =
        getTestServer(String.join("\n", "remoteConfig:", "  s3:", "    compressionType: LZ4"));
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);

    assertEquals(0, runCompress(getS3Async(), compressArgs(indexResource, timeString)));
    assertCompressedSnapshot(indexResource, timeString, "LZ4", "ZSTD");
  }

  @Test
  public void testCompressSnapshot_lz4() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);

    assertEquals(
        0,
        runCompress(
            getS3Async(), compressArgs(indexResource, timeString, "--compressionType=LZ4")));
    assertCompressedSnapshot(indexResource, timeString, null, "LZ4");
  }

  @Test
  public void testCompressSnapshot_lz4StreamingUpload() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);

    AtomicInteger streamingUploads = new AtomicInteger();
    assertEquals(
        0,
        runCompress(
            forwardingAsyncClient(getS3(), streamingUploads),
            compressArgs(
                indexResource, timeString, "--compressionType=LZ4", "--inMemoryThresholdBytes=0")));
    NrtPointState pointState = assertCompressedSnapshot(indexResource, timeString, null, "LZ4");
    assertEquals(pointState.files.size(), streamingUploads.get());
  }

  @Test
  public void testCompressSnapshot_zstdSourceToLz4() throws IOException {
    TestServer server =
        getTestServer(String.join("\n", "remoteConfig:", "  s3:", "    compressionType: ZSTD"));
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);

    assertEquals(
        0,
        runCompress(
            getS3Async(), compressArgs(indexResource, timeString, "--compressionType=LZ4")));
    assertCompressedSnapshot(indexResource, timeString, "ZSTD", "LZ4");
  }

  @Test
  public void testInvalidCompressionType() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    assertNotEquals(
        0,
        runCompress(
            getS3Async(), compressArgs(indexResource, timeString, "--compressionType=SNAPPY")));
  }

  @Test
  public void testRestoreCompressedSnapshot() throws IOException {
    assertRestoreCompressedSnapshot("--compressionLevel=-1");
  }

  @Test
  public void testRestoreLz4CompressedSnapshot() throws IOException {
    assertRestoreCompressedSnapshot("--compressionType=LZ4");
  }

  /** Compress a snapshot with the given args, then restore it and verify the index docs. */
  private void assertRestoreCompressedSnapshot(String... extraArgs) throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    assertEquals(0, runCompress(getS3Async(), compressArgs(indexResource, timeString, extraArgs)));

    String restoreId = TimeStringUtils.generateTimeStringMs();
    RestoreCommand restoreCommand = new RestoreCommand();
    restoreCommand.setS3ClientBundle(new S3Util.S3ClientBundle(getS3(), getS3Async()));
    int exitCode =
        new CommandLine(restoreCommand)
            .execute(
                "--restoreServiceName=" + SERVICE_NAME,
                "--restoreIndexName=restore_index",
                "--restoreIndexId=" + restoreId,
                "--bucketName=" + TEST_BUCKET,
                "--snapshotRoot=" + DEST_ROOT,
                "--snapshotIndexIdentifier=" + indexResource,
                "--snapshotTimeString=" + timeString);
    assertEquals(0, exitCode);

    server.createIndex(
        CreateIndexRequest.newBuilder()
            .setIndexName("restore_index")
            .setExistsWithId(restoreId)
            .build());
    server.startPrimaryIndex(
        "restore_index",
        -1,
        RestoreIndex.newBuilder()
            .setServiceName(SERVICE_NAME)
            .setResourceName("restore_index")
            .build());
    server.verifySimpleDocs("restore_index", 2);
  }

  @Test
  public void testDestinationExists() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    assertEquals(0, runCompress(getS3Async(), compressArgs(indexResource, timeString)));
    assertNotEquals(0, runCompress(getS3Async(), compressArgs(indexResource, timeString)));
  }

  @Test
  public void testDestinationSameAsSource() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    String[] args =
        new String[] {
          "--bucketName=" + TEST_BUCKET,
          "--snapshotServiceName=" + SERVICE_NAME,
          "--snapshotIndexIdentifier=" + indexResource,
          "--snapshotTimeString=" + timeString,
          "--destSnapshotRoot=" + SNAPSHOT_ROOT
        };
    assertNotEquals(0, runCompress(getS3Async(), args));
  }

  @Test
  public void testInvalidCompressionLevel() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    assertNotEquals(
        0,
        runCompress(
            getS3Async(), compressArgs(indexResource, timeString, "--compressionLevel=23")));
    assertFalse(
        S3Util.doesKeyExist(
            getS3(),
            TEST_BUCKET,
            BackupCommandUtils.getSnapshotIndexMetadataKey(
                DEST_ROOT + "/", indexResource, timeString)));
  }

  /**
   * Verify the compressed snapshot contains every source file, compressed with the expected type,
   * along with the updated point state, index state, and metadata.
   *
   * @param sourceCompressionType compression type of the source snapshot files, or null if
   *     uncompressed
   * @param destCompressionType expected compression type of the compressed snapshot files
   * @return compressed snapshot point state
   */
  private NrtPointState assertCompressedSnapshot(
      String indexResource,
      String timeString,
      String sourceCompressionType,
      String destCompressionType)
      throws IOException {
    S3Client s3 = getS3();
    String sourceDataRoot =
        BackupCommandUtils.getSnapshotIndexDataRoot(SNAPSHOT_ROOT + "/", indexResource, timeString);
    String destDataRoot =
        BackupCommandUtils.getSnapshotIndexDataRoot(DEST_ROOT + "/", indexResource, timeString);
    NrtPointState sourcePointState =
        RemoteUtils.pointStateFromUtf8(
            readObject(s3, sourceDataRoot + BackupCommandUtils.SNAPSHOT_POINT_STATE));
    NrtPointState destPointState =
        RemoteUtils.pointStateFromUtf8(
            readObject(s3, destDataRoot + BackupCommandUtils.SNAPSHOT_POINT_STATE));

    assertEquals(sourcePointState.version, destPointState.version);
    assertEquals(sourcePointState.files.keySet(), destPointState.files.keySet());
    FileCompressor sourceCompressor =
        sourceCompressionType == null ? null : compressorFor(sourceCompressionType);
    FileCompressor destCompressor = compressorFor(destCompressionType);
    long totalCompressed = 0;
    for (Map.Entry<String, NrtFileMetaData> entry : destPointState.files.entrySet()) {
      NrtFileMetaData sourceMeta = sourcePointState.files.get(entry.getKey());
      NrtFileMetaData destMeta = entry.getValue();
      assertEquals(sourceCompressionType, sourceMeta.compressionType);
      assertEquals(destCompressionType, destMeta.compressionType);
      assertEquals(sourceMeta.length, destMeta.length);
      assertEquals(sourceMeta.checksum, destMeta.checksum);

      String backendFileName = S3Backend.getIndexBackendFileName(entry.getKey(), destMeta);
      byte[] sourceBytes = readObject(s3, sourceDataRoot + backendFileName);
      if (sourceCompressor != null) {
        sourceBytes = decompress(sourceCompressor, sourceBytes);
      }
      byte[] destBytes = readObject(s3, destDataRoot + backendFileName);
      assertNotNull(destMeta.compressedLength);
      assertEquals(destBytes.length, destMeta.compressedLength.longValue());
      assertArrayEquals(sourceBytes, decompress(destCompressor, destBytes));
      totalCompressed += destBytes.length;
    }

    assertArrayEquals(
        readObject(s3, sourceDataRoot + BackupCommandUtils.SNAPSHOT_INDEX_STATE),
        readObject(s3, destDataRoot + BackupCommandUtils.SNAPSHOT_INDEX_STATE));

    SnapshotMetadata metadata =
        OBJECT_MAPPER.readValue(
            readObject(
                s3,
                BackupCommandUtils.getSnapshotIndexMetadataKey(
                    DEST_ROOT + "/", indexResource, timeString)),
            SnapshotMetadata.class);
    assertEquals(SERVICE_NAME, metadata.getServiceName());
    assertEquals(indexResource, metadata.getIndexName());
    assertEquals(timeString, metadata.getTimeStringMs());
    assertEquals(totalCompressed, metadata.getIndexSizeBytes());
    assertTrue(totalCompressed > 0);
    return destPointState;
  }

  private static FileCompressor compressorFor(String compressionType) {
    return switch (compressionType) {
      case "LZ4" -> new LZ4FileCompressor();
      case "ZSTD" -> new ZstdFileCompressor(3, 0);
      default -> throw new IllegalArgumentException("Unknown compression type: " + compressionType);
    };
  }

  private static byte[] decompress(FileCompressor compressor, byte[] data) throws IOException {
    try (InputStream input = compressor.decompressStream(new ByteArrayInputStream(data))) {
      return IOUtils.toByteArray(input);
    }
  }

  private static byte[] readObject(S3Client s3, String key) throws IOException {
    try (InputStream input =
        s3.getObject(GetObjectRequest.builder().bucket(TEST_BUCKET).key(key).build())) {
      return IOUtils.toByteArray(input);
    }
  }

  /**
   * S3Mock cannot accept request bodies of unknown length, so the streaming upload is tested with
   * an async client that collects each request body and writes it to S3Mock with the sync client.
   */
  private static S3AsyncClient forwardingAsyncClient(S3Client s3, AtomicInteger uploadCount) {
    return mock(
        S3AsyncClient.class,
        invocation -> {
          if (!CompletableFuture.class.isAssignableFrom(invocation.getMethod().getReturnType())) {
            return Mockito.RETURNS_DEFAULTS.answer(invocation);
          }
          PutObjectRequest request = null;
          AsyncRequestBody body = null;
          for (Object arg : invocation.getArguments()) {
            if (arg instanceof PutObjectRequest putObjectRequest) {
              request = putObjectRequest;
            } else if (arg instanceof AsyncRequestBody asyncRequestBody) {
              body = asyncRequestBody;
            }
          }
          if (request == null || body == null) {
            return CompletableFuture.failedFuture(
                new RuntimeException("unexpected call: " + invocation.getMethod().getName()));
          }
          String key = request.key();
          CompletableFuture<PutObjectResponse> result = new CompletableFuture<>();
          ByteArrayOutputStream received = new ByteArrayOutputStream();
          body.subscribe(
              new Subscriber<ByteBuffer>() {
                @Override
                public void onSubscribe(Subscription s) {
                  s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(ByteBuffer buffer) {
                  byte[] bytes = new byte[buffer.remaining()];
                  buffer.get(bytes);
                  received.write(bytes, 0, bytes.length);
                }

                @Override
                public void onError(Throwable t) {
                  result.completeExceptionally(t);
                }

                @Override
                public void onComplete() {
                  try {
                    s3.putObject(
                        PutObjectRequest.builder().bucket(TEST_BUCKET).key(key).build(),
                        RequestBody.fromBytes(received.toByteArray()));
                    uploadCount.incrementAndGet();
                    result.complete(PutObjectResponse.builder().build());
                  } catch (Throwable t) {
                    result.completeExceptionally(t);
                  }
                }
              });
          return result;
        });
  }
}
