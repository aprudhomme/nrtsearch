package com.yelp.nrtsearch.server.luceneserver.nrt;

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

public class StateManager {
  protected static final ObjectMapper MAPPER = new ObjectMapper();
  protected final UUID ephemeralId = UUID.randomUUID();

  private static final String BASE_NODE = "nrtsearch";
  private static final String REPLICAS_NODE = "replicas";
  private static final String PRIMARY_NODE = "primary";
  protected static final String MERGES_NODE = "merges";
  private static final String ACTIVE_STATE_NODE = "active_state";

  private final String cluster = "test-cluster";
  private final String indexName;
  private final String shardOrd;

  private final String basePath;
  private final String replicasBasePath;

  private ZooKeeper zk;
  private ZookeeperConnectionWatcher connectionWatcher;

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

  public StateManager(String indexName, int shardOrd) throws IOException {
    this.indexName = indexName;
    this.shardOrd = String.valueOf(shardOrd);

    basePath = "/" + BASE_NODE + "/" + cluster + "/" + indexName + "/" + shardOrd;
    replicasBasePath = basePath + "/" + REPLICAS_NODE;

    connectionWatcher = new ZookeeperConnectionWatcher();
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
    return basePath + "/" + PRIMARY_NODE;
  }

  protected String getActiveStatePath() {
    return basePath + "/" + ACTIVE_STATE_NODE;
  }

  protected void createBaseNodes() {
    try {
      ensurePath(BASE_NODE, cluster, indexName, shardOrd, REPLICAS_NODE);
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
    zk.create(
        path,
        data,
        ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
  }

  protected void assertNodes() {
    assertNodePath(BASE_NODE, cluster, indexName, this.shardOrd, REPLICAS_NODE);
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
}
