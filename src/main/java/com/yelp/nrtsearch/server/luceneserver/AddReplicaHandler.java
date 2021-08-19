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

import com.yelp.nrtsearch.server.grpc.AddReplicaRequest;
import com.yelp.nrtsearch.server.grpc.AddReplicaResponse;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

public class AddReplicaHandler implements Handler<AddReplicaRequest, AddReplicaResponse> {
  private static final Object resizeThreadPoolLock = new Object();

  @Override
  public AddReplicaResponse handle(IndexState indexState, AddReplicaRequest addReplicaRequest) {
    ShardState shardState = indexState.getShard(0);
    if (shardState.isPrimary() == false) {
      throw new IllegalArgumentException(
          "index \"" + indexState.name + "\" was not started or is not a primary");
    }

    if (!isValidMagicHeader(addReplicaRequest.getMagicNumber())) {
      throw new RuntimeException("AddReplica invoked with Invalid Magic Number");
    }
    try {
      shardState.nrtPrimaryNode.addReplica(
          addReplicaRequest.getReplicaId(),
          // channel for primary to talk to replica
          new ReplicationServerClient(
              addReplicaRequest.getHostName(), addReplicaRequest.getPort()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (indexState.globalState.configuration.getAutoReplicationThreadPoolSizing()) {
      resizeReplicationThreadPool(
          () -> shardState.nrtPrimaryNode.getNodesInfo().size(),
          indexState.globalState.getReplicationThreadPoolExecutor());
    }
    return AddReplicaResponse.newBuilder().setOk("ok").build();
  }

  static void resizeReplicationThreadPool(
      Supplier<Integer> numReplicasSupplier, ThreadPoolExecutor replicationThreadPool) {
    // serialize updates if multiple replicas are being added concurrently to ensure the latest
    // value gets set
    synchronized (resizeThreadPoolLock) {
      int numReplicas = numReplicasSupplier.get();
      int newMaxThreads = computeThreadsForReplicas(numReplicas);
      int currentMaxThreads = replicationThreadPool.getMaximumPoolSize();
      // the call order depends on if the size is increasing or decreasing
      if (newMaxThreads > currentMaxThreads) {
        replicationThreadPool.setMaximumPoolSize(newMaxThreads);
        replicationThreadPool.setCorePoolSize(newMaxThreads);
      } else if (currentMaxThreads > newMaxThreads) {
        replicationThreadPool.setCorePoolSize(newMaxThreads);
        replicationThreadPool.setMaximumPoolSize(newMaxThreads);
      }
    }
  }

  /**
   * We need at least one thread per replica to handle file copy, plus a few extra for short lived
   * requests (getting primary node list, getting copy state, etc).
   */
  static int computeThreadsForReplicas(int numReplicas) {
    return numReplicas + (numReplicas / 10) + 1;
  }
}
