/*
 * Copyright 2024 Yelp Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.config.IndexStartConfig;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReplicationIndexNotFoundTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private TestServer getPrimary() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.LOCAL)
            .withAdditionalConfig("verifyReplicationIndexId: true")
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);
    return primaryServer;
  }

  private void assertException(StatusRuntimeException e) {
    assertEquals(Status.FAILED_PRECONDITION.getCode(), e.getStatus().getCode());
    assertEquals("FAILED_PRECONDITION: Index not found: invalid_index", e.getMessage());
  }

  @Test
  public void testIndexNotFound_addReplicas() throws IOException {
    TestServer primary = getPrimary();
    try {
      primary
          .getReplicationClient()
          .getBlockingStub()
          .addReplicas(
              AddReplicaRequest.newBuilder()
                  .setIndexName("invalid_index")
                  .setHostName("host")
                  .setPort(1234)
                  .setMagicNumber(ReplicationServerClient.BINARY_MAGIC)
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertException(e);
    }
  }

  @Test
  public void testIndexNotFound_recvRawFile() throws IOException {
    TestServer primary = getPrimary();
    try {
      Iterator<RawFileChunk> it =
          primary
              .getReplicationClient()
              .getBlockingStub()
              .recvRawFile(
                  FileInfo.newBuilder()
                      .setIndexName("invalid_index")
                      .setFileName("file")
                      .setFpStart(0)
                      .build());
      it.next();
      fail();
    } catch (StatusRuntimeException e) {
      assertException(e);
    }
  }

  @Test
  public void testIndexNotFound_recvRawFileV2() throws IOException {
    TestServer primary = getPrimary();
    AtomicReference<Throwable> t = new AtomicReference<>();
    final AtomicReference<Boolean> done = new AtomicReference<>(false);
    StreamObserver<FileInfo> so =
        primary
            .getReplicationClient()
            .getAsyncStub()
            .recvRawFileV2(
                new StreamObserver<>() {
                  @Override
                  public void onNext(RawFileChunk rawFileChunk) {
                    synchronized (done) {
                      done.set(true);
                      done.notifyAll();
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {
                    System.out.println("error received: " + throwable);
                    t.set(throwable);
                    synchronized (done) {
                      done.set(true);
                      done.notifyAll();
                    }
                  }

                  @Override
                  public void onCompleted() {
                    synchronized (done) {
                      done.set(true);
                      done.notifyAll();
                    }
                  }
                });
    so.onNext(
        FileInfo.newBuilder()
            .setIndexName("invalid_index")
            .setFileName("file")
            .setFpStart(0)
            .build());
    synchronized (done) {
      while (true) {
        if (done.get()) {
          break;
        }
        try {
          done.wait(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    assertTrue(t.get() instanceof StatusRuntimeException);
    assertException((StatusRuntimeException) t.get());
  }

  @Test
  public void testIndexNotFound_recvCopyState() throws IOException {
    TestServer primary = getPrimary();
    try {
      primary
          .getReplicationClient()
          .getBlockingStub()
          .recvCopyState(
              CopyStateRequest.newBuilder()
                  .setIndexName("invalid_index")
                  .setMagicNumber(ReplicationServerClient.BINARY_MAGIC)
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertException(e);
    }
  }

  @Test
  public void testIndexNotFound_copyFiles() throws IOException {
    TestServer primary = getPrimary();
    try {
      Iterator<TransferStatus> it =
          primary
              .getReplicationClient()
              .getBlockingStub()
              .copyFiles(
                  CopyFiles.newBuilder()
                      .setIndexName("invalid_index")
                      .setMagicNumber(ReplicationServerClient.BINARY_MAGIC)
                      .build());
      it.next();
      fail();
    } catch (StatusRuntimeException e) {
      assertException(e);
    }
  }

  @Test
  public void testIndexNotFound_newNRTPoint() throws IOException {
    TestServer primary = getPrimary();
    try {
      primary
          .getReplicationClient()
          .getBlockingStub()
          .newNRTPoint(
              NewNRTPoint.newBuilder()
                  .setIndexName("invalid_index")
                  .setMagicNumber(ReplicationServerClient.BINARY_MAGIC)
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertException(e);
    }
  }
}
