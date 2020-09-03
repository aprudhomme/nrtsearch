/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class CommitBatcher implements Closeable {

  @Override
  public void close() throws IOException {
    synchronized (commitThread) {
      commitThread.done = true;
      commitThread.notify();
    }
    try {
      commitThread.join();
    } catch (InterruptedException ignored) {
    }
  }

  public static class BatchCommitStatus {
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile Exception error = null;
    private volatile long generation = -1;

    public void waitForCompletion() throws InterruptedException {
      latch.await();
    }

    public long getGenOrThrow() throws Exception {
      if (error != null) {
        throw error;
      }
      return generation;
    }
  }

  public class BatchCommitThread extends Thread {

    private boolean done = false;
    private boolean startCommit = false;

    @Override
    public void run() {
      while (true) {
        BatchCommitStatus batch;
        synchronized (this) {
          if (done) {
            break;
          }
          if (!startCommit) {
            try {
              this.wait();
            } catch (InterruptedException ignored) {
            }
          }
          if (done) {
            break;
          }
          if (startCommit) {
            batch = nextBatch;
            nextBatch = new BatchCommitStatus();
          } else {
            continue;
          }
        }

        try {
          batch.generation = indexState.commit();
        } catch (Exception e) {
          batch.error = e;
        } finally {
          batch.latch.countDown();
        }
      }
      nextBatch.error = new RuntimeException("Server is shutting down");
      nextBatch.latch.countDown();
    }
  }

  private final IndexState indexState;
  private final BatchCommitThread commitThread;
  private BatchCommitStatus nextBatch;

  public CommitBatcher(IndexState indexState) {
    this.indexState = indexState;
    nextBatch = new BatchCommitStatus();
    commitThread = new BatchCommitThread();
    commitThread.start();
  }

  public BatchCommitStatus register() {
    synchronized (commitThread) {
      commitThread.startCommit = true;
      commitThread.notify();
      return nextBatch;
    }
  }
}
