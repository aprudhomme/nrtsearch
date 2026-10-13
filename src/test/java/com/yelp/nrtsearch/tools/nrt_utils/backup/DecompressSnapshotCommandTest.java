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
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.FileCompressor;
import com.yelp.nrtsearch.server.remote.FileCompressorCreator;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.remote.ZstdFileCompressor;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class DecompressSnapshotCommandTest {
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

  /** Output of a command run. */
  private record Result(int exitCode, String out, String err) {}

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

  private String getIndexResource(TestServer server) {
    return server.getGlobalState().getDataResourceForIndex("test_index");
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

  private void compress(String indexResource, String timeString, String... extra) {
    CompressSnapshotCommand command = new CompressSnapshotCommand();
    command.setS3ClientBundle(new S3Util.S3ClientBundle(getS3(), getS3Async()));
    List<String> args =
        new ArrayList<>(
            List.of(
                "--bucketName=" + TEST_BUCKET,
                "--snapshotServiceName=" + SERVICE_NAME,
                "--snapshotIndexIdentifier=" + indexResource,
                "--snapshotTimeString=" + timeString,
                "--destSnapshotRoot=" + DEST_ROOT));
    args.addAll(List.of(extra));
    assertEquals(0, new CommandLine(command).execute(args.toArray(new String[0])));
  }

  private Result decompress(
      String snapshotRoot, String indexResource, String timeString, String... extra) {
    DecompressSnapshotCommand command = new DecompressSnapshotCommand();
    command.setS3ClientBundle(new S3Util.S3ClientBundle(getS3(), getS3Async()));
    List<String> args =
        new ArrayList<>(
            List.of(
                "--bucketName=" + TEST_BUCKET,
                "--snapshotRoot=" + snapshotRoot,
                "--snapshotIndexIdentifier=" + indexResource,
                "--snapshotTimeString=" + timeString));
    args.addAll(List.of(extra));
    CommandLine commandLine = new CommandLine(command);
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
    int exitCode;
    try {
      exitCode = commandLine.execute(args.toArray(new String[0]));
    } finally {
      System.setOut(originalOut);
    }
    String outString = out.toString(StandardCharsets.UTF_8);
    System.out.print(outString);
    return new Result(exitCode, outString, err.toString());
  }

  private NrtPointState getPointState(String snapshotRoot, String indexResource, String timeString)
      throws IOException {
    String dataRoot =
        BackupCommandUtils.getSnapshotIndexDataRoot(snapshotRoot + "/", indexResource, timeString);
    return RemoteUtils.pointStateFromUtf8(
        readObject(dataRoot + BackupCommandUtils.SNAPSHOT_POINT_STATE));
  }

  private byte[] readObject(String key) throws IOException {
    try (InputStream input =
        getS3().getObject(GetObjectRequest.builder().bucket(TEST_BUCKET).key(key).build())) {
      return IOUtils.toByteArray(input);
    }
  }

  @Test
  public void testDecompressZstdSnapshot() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    compress(indexResource, timeString, "--compressionLevel=9");
    int numFiles = getPointState(DEST_ROOT, indexResource, timeString).files.size();

    Result result = decompress(DEST_ROOT, indexResource, timeString);
    assertEquals(result.err(), 0, result.exitCode());
    assertTrue(result.out().contains("compression types (files): {ZSTD=" + numFiles + "}"));
    assertTrue(result.out().contains("files: " + numFiles + ", all verified"));
    assertTrue(result.out().contains("decompress time (summed over files):"));
    assertTrue(result.out().contains("by file type:"));
    assertTrue(result.out().contains("    cfs "));
  }

  @Test
  public void testDecompressLz4Snapshot_tempFiles() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    compress(indexResource, timeString, "--compressionType=LZ4");
    int numFiles = getPointState(DEST_ROOT, indexResource, timeString).files.size();
    File tempDir = folder.newFolder("decompress_temp");

    Result result =
        decompress(
            DEST_ROOT,
            indexResource,
            timeString,
            "--inMemoryThresholdBytes=0",
            "--tempDir=" + tempDir.getAbsolutePath(),
            "--fileConcurrency=2");
    assertEquals(result.err(), 0, result.exitCode());
    assertTrue(result.out().contains("compression types (files): {LZ4=" + numFiles + "}"));
    assertArrayEquals(new String[0], tempDir.list());
  }

  @Test
  public void testDecompressUncompressedSnapshot() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    int numFiles = getPointState(SNAPSHOT_ROOT, indexResource, timeString).files.size();

    Result result = decompress(SNAPSHOT_ROOT, indexResource, timeString);
    assertEquals(result.err(), 0, result.exitCode());
    assertTrue(result.out().contains("compression types (files): {NONE=" + numFiles + "}"));
    assertTrue(result.out().contains("files: " + numFiles + ", all verified"));
  }

  @Test
  public void testDecompressServerCompressedSnapshot() throws IOException {
    TestServer server =
        getTestServer(String.join("\n", "remoteConfig:", "  s3:", "    compressionType: ZSTD"));
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    int numFiles = getPointState(SNAPSHOT_ROOT, indexResource, timeString).files.size();

    Result result = decompress(SNAPSHOT_ROOT, indexResource, timeString);
    assertEquals(result.err(), 0, result.exitCode());
    assertTrue(result.out().contains("compression types (files): {ZSTD=" + numFiles + "}"));
  }

  @Test
  public void testChecksumMismatch() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    compress(indexResource, timeString);
    replaceLargestFile(
        indexResource,
        timeString,
        data -> {
          byte[] corrupted = Arrays.copyOf(data, data.length);
          corrupted[corrupted.length / 2] ^= 1;
          return corrupted;
        });

    Result result = decompress(DEST_ROOT, indexResource, timeString);
    assertEquals(1, result.exitCode());
    assertTrue(result.err(), result.err().contains("checksum mismatch"));
  }

  @Test
  public void testLengthMismatch() throws IOException {
    TestServer server = getTestServer("");
    String indexResource = getIndexResource(server);
    String timeString = snapshot(server);
    compress(indexResource, timeString);
    replaceLargestFile(indexResource, timeString, data -> Arrays.copyOf(data, data.length - 1));

    Result result = decompress(DEST_ROOT, indexResource, timeString);
    assertEquals(1, result.exitCode());
    assertTrue(result.err(), result.err().contains("size mismatch"));
  }

  @Test
  public void testMissingSnapshot() {
    Result result = decompress(DEST_ROOT, "missing_index-123", "20260101000000000");
    assertEquals(1, result.exitCode());
    assertTrue(result.err(), result.err().contains("Metadata file does not exist"));
  }

  /**
   * Replace the largest file in the compressed snapshot with zstd compressed data produced by
   * applying the modifier to its uncompressed content.
   */
  private void replaceLargestFile(
      String indexResource, String timeString, UnaryOperator<byte[]> modifier) throws IOException {
    NrtPointState pointState = getPointState(DEST_ROOT, indexResource, timeString);
    Map.Entry<String, NrtFileMetaData> largest =
        pointState.files.entrySet().stream()
            .max(Comparator.comparingLong(e -> e.getValue().length))
            .orElseThrow();
    String key =
        BackupCommandUtils.getSnapshotIndexDataRoot(DEST_ROOT + "/", indexResource, timeString)
            + S3Backend.getIndexBackendFileName(largest.getKey(), largest.getValue());
    FileCompressor zstd =
        new ZstdFileCompressor(
            ZstdFileCompressor.DEFAULT_LEVEL, ZstdFileCompressor.DEFAULT_WORKERS);
    byte[] data;
    try (InputStream input = zstd.decompressStream(new ByteArrayInputStream(readObject(key)))) {
      data = IOUtils.toByteArray(input);
    }
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (OutputStream output = zstd.compressStream(compressed)) {
      output.write(modifier.apply(data));
    }
    getS3()
        .putObject(
            PutObjectRequest.builder().bucket(TEST_BUCKET).key(key).build(),
            RequestBody.fromBytes(compressed.toByteArray()));
  }
}
