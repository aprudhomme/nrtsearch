/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.handler;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.FileCopyConfig;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.FileInfo;
import com.yelp.nrtsearch.server.grpc.RawFileChunk;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RecvRawFileV2HandlerTest {
  private static final String INDEX_NAME = "test_index";
  private static final String INDEX_ID = "test_id";
  private static final String FILE_NAME = "_0.cfs";
  // Held open for the whole test, like the searcher's segment reader
  private static final String HOLDER_FILE_NAME = "_0.cfe";
  private static final int CHUNK_SIZE = 1024;
  private static final int NUM_CHUNKS = 50;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private MMapDirectory directory;
  private IndexInput holder;
  private byte[] fileContent;
  private final ExecutorService executor = Executors.newFixedThreadPool(2);

  @Before
  public void setup() throws IOException {
    directory = new MMapDirectory(folder.getRoot().toPath());
    directory.setGroupingFunction(MMapDirectory.GROUP_BY_SEGMENT);

    // not chunk aligned, so the last chunk is partial
    fileContent = new byte[CHUNK_SIZE * NUM_CHUNKS + 100];
    new Random(42).nextBytes(fileContent);
    try (IndexOutput out = directory.createOutput(FILE_NAME, IOContext.DEFAULT)) {
      out.writeBytes(fileContent, fileContent.length);
    }
    try (IndexOutput out = directory.createOutput(HOLDER_FILE_NAME, IOContext.DEFAULT)) {
      out.writeBytes(fileContent, 100);
    }
    holder = directory.openInput(HOLDER_FILE_NAME, IOContext.DEFAULT);
  }

  @After
  public void cleanup() throws IOException {
    executor.shutdownNow();
    holder.close();
    directory.close();
  }

  private RecvRawFileV2Handler createHandler(int ackEvery, int maxInFlight) throws IOException {
    GlobalState mockGlobalState = mock(GlobalState.class);
    NrtsearchConfig mockConfig = mock(NrtsearchConfig.class);
    when(mockGlobalState.getConfiguration()).thenReturn(mockConfig);
    when(mockConfig.getFileCopyConfig())
        .thenReturn(new FileCopyConfig(true, CHUNK_SIZE, ackEvery, maxInFlight));

    ShardState mockShardState = mock(ShardState.class);
    mockShardState.indexDir = directory;
    IndexState mockIndexState = mock(IndexState.class);
    when(mockIndexState.getShard(0)).thenReturn(mockShardState);
    IndexStateManager mockStateManager = mock(IndexStateManager.class);
    when(mockStateManager.getCurrent()).thenReturn(mockIndexState);
    when(mockStateManager.getIndexId()).thenReturn(INDEX_ID);
    when(mockGlobalState.getIndexStateManager(INDEX_NAME)).thenReturn(mockStateManager);
    return new RecvRawFileV2Handler(mockGlobalState, true);
  }

  @Test
  public void testCopyAcksOnDifferentThreads() throws Exception {
    assertCopy(1, 2, 0);
  }

  @Test
  public void testCopyMultipleChunksPerAck() throws Exception {
    assertCopy(5, 10, 0);
  }

  @Test
  public void testCopySingleWindow() throws Exception {
    assertCopy(1000, 2000, 0);
  }

  @Test
  public void testCopyFromOffset() throws Exception {
    assertCopy(3, 6, CHUNK_SIZE * 7 + 13);
  }

  private void assertCopy(int ackEvery, int maxInFlight, long fpStart) throws Exception {
    RecvRawFileV2Handler handler = createHandler(ackEvery, maxInFlight);
    TestResponseObserver responseObserver = new TestResponseObserver();
    StreamObserver<FileInfo> requestObserver = handler.handle(responseObserver);

    requestObserver.onNext(
        FileInfo.newBuilder()
            .setIndexName(INDEX_NAME)
            .setIndexId(INDEX_ID)
            .setFileName(FILE_NAME)
            .setFpStart(fpStart)
            .build());

    // Send acks from alternating threads, like gRPC callbacks
    int processed = 0;
    int calls = 0;
    while (!responseObserver.completed) {
      assertNull(responseObserver.error);
      List<RawFileChunk> chunks = responseObserver.chunks;
      int lastAck = -1;
      for (int i = processed; i < chunks.size(); ++i) {
        if (chunks.get(i).getAck()) {
          lastAck = chunks.get(i).getSeqNum();
        }
      }
      processed = chunks.size();
      assertTrue("No ack chunk and transfer not complete", lastAck > 0);
      int ackSeq = lastAck;
      executor
          .submit(() -> requestObserver.onNext(FileInfo.newBuilder().setAckSeqNum(ackSeq).build()))
          .get();
      calls++;
    }
    requestObserver.onCompleted();
    assertNull(responseObserver.error);

    ByteArrayOutputStream received = new ByteArrayOutputStream();
    int expectedSeq = 1;
    for (RawFileChunk chunk : responseObserver.chunks) {
      assertEquals(expectedSeq++, chunk.getSeqNum());
      assertEquals(chunk.getSeqNum() % ackEvery == 0, chunk.getAck());
      chunk.getContent().writeTo(received);
    }
    byte[] expected = new byte[fileContent.length - (int) fpStart];
    System.arraycopy(fileContent, (int) fpStart, expected, 0, expected.length);
    assertArrayEquals(expected, received.toByteArray());
    if (ackEvery < NUM_CHUNKS) {
      assertTrue("Expected multiple ack windows", calls > 1);
    }
    assertNoFileMappings();
  }

  /** The copied file must not remain mapped while other segment files are open. Linux only. */
  private void assertNoFileMappings() throws IOException {
    Path maps = Path.of("/proc/self/maps");
    if (!Files.exists(maps)) {
      return;
    }
    Path filePath = folder.getRoot().toPath().resolve(FILE_NAME).toRealPath();
    long count =
        Files.readAllLines(maps).stream().filter(l -> l.endsWith(filePath.toString())).count();
    assertEquals(0, count);
  }

  private static class TestResponseObserver implements StreamObserver<RawFileChunk> {
    final List<RawFileChunk> chunks = new ArrayList<>();
    volatile boolean completed = false;
    volatile Throwable error;

    @Override
    public synchronized void onNext(RawFileChunk value) {
      chunks.add(value);
    }

    @Override
    public void onError(Throwable t) {
      error = t;
    }

    @Override
    public void onCompleted() {
      completed = true;
    }
  }
}
