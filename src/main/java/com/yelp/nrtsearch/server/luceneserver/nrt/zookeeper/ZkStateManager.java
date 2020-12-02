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
package com.yelp.nrtsearch.server.luceneserver.nrt.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.UUID;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkStateManager {
  protected static final ObjectMapper MAPPER = new ObjectMapper();
  protected final UUID ephemeralId = UUID.randomUUID();

  private static final String BASE_NODE = "nrtsearch";
  private static final String REPLICAS_NODE = "replicas";
  private static final String PRIMARY_NODE = "primary";
  private static final String MERGES_NODE = "merges";
  private static final String ACTIVE_STATE_NODE = "active_state";

  private final String serviceName;
  private final String indexName;
  private final String shardOrd;

  private final String basePath;
  private final String replicasBasePath;
  private final String primaryPath;
  private final String activeStatePath;
  private final String mergesPath;

  private final ZooKeeper zk;

  protected static class ZookeeperConnectionWatcher implements Watcher {

    boolean established = false;
    RuntimeException error;

    @Override
    public void process(WatchedEvent event) {
      System.out.println("Processing zk connection event: " + event);
      synchronized (this) {
        switch (event.getState()) {
          case SyncConnected:
            established = true;
            error = null;
            break;
          case Closed:
          case Expired:
          case AuthFailed:
          case Disconnected:
            established = false;
            error = new RuntimeException("Unrecoverable zk connection state: " + event.getState());
            break;
          default:
        }
        this.notifyAll();
      }
    }

    public synchronized void waitForConnection() {
      while (!established && error == null) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted", e);
        }
      }
      if (error != null) {
        throw error;
      }
    }
  }

  public ZkStateManager(String indexName, int shardOrd, String serviceName) throws IOException {
    this.indexName = indexName;
    this.shardOrd = String.valueOf(shardOrd);
    this.serviceName = serviceName;

    basePath = "/" + BASE_NODE + "/" + serviceName + "/" + indexName + "/" + shardOrd;
    replicasBasePath = basePath + "/" + REPLICAS_NODE;
    primaryPath = basePath + "/" + PRIMARY_NODE;
    activeStatePath = basePath + "/" + ACTIVE_STATE_NODE;
    mergesPath = basePath + "/" + MERGES_NODE;

    ZookeeperConnectionWatcher connectionWatcher = new ZookeeperConnectionWatcher();
    zk = new ZooKeeper("localhost:2181", 10000, connectionWatcher);
    connectionWatcher.waitForConnection();
    System.out.println("Zk connection established");
  }

  protected ZooKeeper getZk() {
    return zk;
  }

  protected String getBasePath() {
    return basePath;
  }

  protected String getReplicasBasePath() {
    return replicasBasePath;
  }

  protected String getPrimaryPath() {
    return primaryPath;
  }

  protected String getActiveStatePath() {
    return activeStatePath;
  }

  protected String getMergeStatePath() {
    return mergesPath;
  }

  protected boolean isMergeStateNode(String node) {
    return node.startsWith(MERGES_NODE);
  }

  protected void createBaseNodes() {
    try {
      ensurePath(BASE_NODE, serviceName, indexName, shardOrd, REPLICAS_NODE);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Error creating base znodes", e);
    }
  }

  private void ensurePath(String... path) throws KeeperException, InterruptedException {
    String currentPath = "";
    for (String node : path) {
      currentPath += ("/" + node);
      Stat stat = zk.exists(currentPath, false);
      if (stat == null) {
        createNode(currentPath, new byte[0]);
      }
    }
  }

  private void createNode(String path, byte[] data) throws KeeperException, InterruptedException {
    System.out.println("Creating node: " + path);
    zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  protected void assertNodes() {
    assertNodePath(BASE_NODE, serviceName, indexName, this.shardOrd, REPLICAS_NODE);
  }

  private void assertNodePath(String... path) {
    try {
      String currentPath = "";
      for (String node : path) {
        currentPath += ("/" + node);
        Stat stat = zk.exists(currentPath, false);
        if (stat == null) {
          throw new IllegalStateException("ZK node path does not exist: " + currentPath);
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Error asserting path: " + e);
    }
  }

  public void close() {
    try {
      zk.close();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted closing ZK connection", e);
    }
  }
}
