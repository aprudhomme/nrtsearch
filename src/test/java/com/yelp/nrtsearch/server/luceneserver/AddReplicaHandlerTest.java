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
package com.yelp.nrtsearch.server.luceneserver;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.Test;

public class AddReplicaHandlerTest {

  @Test
  public void testThreadPoolResize() {
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(100);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            10, 10, 0, TimeUnit.SECONDS, queue, new NamedThreadFactory("TestExecutor"));
    try {
      assertSizes(threadPoolExecutor, 10);
      AddReplicaHandler.resizeReplicationThreadPool(() -> 12, threadPoolExecutor);
      assertSizes(threadPoolExecutor, 14);
      AddReplicaHandler.resizeReplicationThreadPool(() -> 0, threadPoolExecutor);
      assertSizes(threadPoolExecutor, 1);
      AddReplicaHandler.resizeReplicationThreadPool(() -> 1, threadPoolExecutor);
      assertSizes(threadPoolExecutor, 2);
      AddReplicaHandler.resizeReplicationThreadPool(() -> 5, threadPoolExecutor);
      assertSizes(threadPoolExecutor, 6);
      AddReplicaHandler.resizeReplicationThreadPool(() -> 100, threadPoolExecutor);
      assertSizes(threadPoolExecutor, 111);
      AddReplicaHandler.resizeReplicationThreadPool(() -> 10, threadPoolExecutor);
      assertSizes(threadPoolExecutor, 12);
    } finally {
      threadPoolExecutor.shutdown();
    }
  }

  private void assertSizes(ThreadPoolExecutor executor, int size) {
    assertEquals(size, executor.getMaximumPoolSize());
    assertEquals(size, executor.getCorePoolSize());
  }
}
