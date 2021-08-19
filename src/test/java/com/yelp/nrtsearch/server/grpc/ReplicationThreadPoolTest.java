/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc;

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.assertEquals;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.utils.Archiver;
import com.yelp.nrtsearch.server.utils.ArchiverImpl;
import com.yelp.nrtsearch.server.utils.Tar;
import com.yelp.nrtsearch.server.utils.TarImpl;
import io.findify.s3mock.S3Mock;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReplicationThreadPoolTest {
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private GrpcServer luceneServerPrimary;
  private GrpcServer replicationServerPrimary;

  private GrpcServer luceneServerSecondary;
  private GrpcServer replicationServerSecondary;

  private final String BUCKET_NAME = "archiver-unittest";
  private Archiver archiver;
  private S3Mock api;
  private AmazonS3 s3;
  private Path s3Directory;
  private Path archiverDirectory;

  @After
  public void tearDown() throws IOException {
    api.shutdown();
    luceneServerPrimary.getGlobalState().close();
    luceneServerSecondary.getGlobalState().close();
    rmDir(Paths.get(luceneServerPrimary.getIndexDir()).getParent());
    rmDir(Paths.get(luceneServerSecondary.getIndexDir()).getParent());
  }

  public void setUp(boolean autoResizeThreadPool) throws IOException {
    // setup S3 for backup/restore
    s3Directory = folder.newFolder("s3").toPath();
    archiverDirectory = folder.newFolder("archiver").toPath();
    api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
    api.start();
    s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint("http://127.0.0.1:8011");
    s3.createBucket(BUCKET_NAME);
    archiver =
        new ArchiverImpl(s3, BUCKET_NAME, archiverDirectory, new TarImpl(Tar.CompressionMode.LZ4));

    String extraConfig =
        String.join("\n", "threadPoolConfiguration:", "  maxGrpcReplicationserverThreads: 5");
    if (autoResizeThreadPool) {
      extraConfig = extraConfig + "\nautoReplicationThreadPoolSizing: true";
    }

    // set up primary servers
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerPrimaryConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.PRIMARY, folder.getRoot(), extraConfig);
    GlobalState globalStatePrimary = new GlobalState(luceneServerPrimaryConfiguration);
    luceneServerPrimary =
        new GrpcServer(
            grpcCleanup,
            luceneServerPrimaryConfiguration,
            folder,
            false,
            globalStatePrimary,
            luceneServerPrimaryConfiguration.getIndexDir(),
            testIndex,
            globalStatePrimary.getPort(),
            archiver);
    replicationServerPrimary =
        new GrpcServer(
            grpcCleanup,
            luceneServerPrimaryConfiguration,
            folder,
            true,
            globalStatePrimary,
            luceneServerPrimaryConfiguration.getIndexDir(),
            testIndex,
            luceneServerPrimaryConfiguration.getReplicationPort(),
            archiver);
    // set up secondary servers
    LuceneServerConfiguration luceneServerSecondaryConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.REPLICA, folder.getRoot());
    GlobalState globalStateSecondary = new GlobalState(luceneServerSecondaryConfiguration);

    luceneServerSecondary =
        new GrpcServer(
            grpcCleanup,
            luceneServerSecondaryConfiguration,
            folder,
            false,
            globalStateSecondary,
            luceneServerSecondaryConfiguration.getIndexDir(),
            testIndex,
            globalStateSecondary.getPort(),
            archiver);
    replicationServerSecondary =
        new GrpcServer(
            grpcCleanup,
            luceneServerSecondaryConfiguration,
            folder,
            true,
            globalStateSecondary,
            luceneServerSecondaryConfiguration.getIndexDir(),
            testIndex,
            globalStateSecondary.getReplicationPort(),
            archiver);
  }

  @Test
  public void testAutoResizeDisabled() throws IOException, InterruptedException {
    setUp(false);

    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    assertEquals(
        5,
        luceneServerPrimary
            .getGlobalState()
            .getReplicationThreadPoolExecutor()
            .getMaximumPoolSize());
    assertEquals(
        5,
        luceneServerPrimary.getGlobalState().getReplicationThreadPoolExecutor().getCorePoolSize());
    testServerPrimary.addDocuments();
    // refresh (also sends NRTPoint to replicas, but none started at this point)
    luceneServerPrimary
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());
    // startIndex replica
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
    assertEquals(
        5,
        luceneServerPrimary
            .getGlobalState()
            .getReplicationThreadPoolExecutor()
            .getMaximumPoolSize());
    assertEquals(
        5,
        luceneServerPrimary.getGlobalState().getReplicationThreadPoolExecutor().getCorePoolSize());
  }

  @Test
  public void testAutoResizeEnabled() throws IOException, InterruptedException {
    setUp(true);

    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    assertEquals(
        5,
        luceneServerPrimary
            .getGlobalState()
            .getReplicationThreadPoolExecutor()
            .getMaximumPoolSize());
    assertEquals(
        5,
        luceneServerPrimary.getGlobalState().getReplicationThreadPoolExecutor().getCorePoolSize());
    testServerPrimary.addDocuments();
    // refresh (also sends NRTPoint to replicas, but none started at this point)
    luceneServerPrimary
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());
    // startIndex replica
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
    assertEquals(
        2,
        luceneServerPrimary
            .getGlobalState()
            .getReplicationThreadPoolExecutor()
            .getMaximumPoolSize());
    assertEquals(
        2,
        luceneServerPrimary.getGlobalState().getReplicationThreadPoolExecutor().getCorePoolSize());
  }
}
