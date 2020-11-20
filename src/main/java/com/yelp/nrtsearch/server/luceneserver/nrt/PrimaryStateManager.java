package com.yelp.nrtsearch.server.luceneserver.nrt;

import com.yelp.nrtsearch.server.luceneserver.nrt.MergeState.ReplicaMergeState;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

public class PrimaryStateManager extends StateManager {
  private final Map<String, ReplicaState> replicaStateMap = new HashMap<>();
  boolean isPrimary = false;

  private MergeState localMergeState;
  private String mergeStateNodePath;

  private NrtActiveState activeState;

  public PrimaryStateManager(String indexName, int shardOrd, long primaryGen) throws IOException {
    super(indexName, shardOrd);

    createBaseNodes();

    claimPrimary();
    if (!isPrimary) {
      throw new IllegalStateException("Another node is already the primary");
    }

    System.out.println("I am the primary!");

    assertActiveStateNode();
    getActiveState();

    localMergeState = new MergeState(ephemeralId.toString(), primaryGen, new HashMap<>(),
        new HashMap<>());
    initMergeState();

    // start async call to monitor replicas membership
    getReplicas();
  }

  private void claimPrimary() {
    try {
      while (true) {
        try {
          getZk().create(getPrimaryPath(), ephemeralId.toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
          isPrimary = true;
          break;
        } catch (NodeExistsException e) {
          isPrimary = false;
          break;
        } catch (ConnectionLossException ignored) {
        } catch (KeeperException e) {
          throw new RuntimeException("Error claiming primary: " + e);
        }
        if (checkPrimary()) {
          break;
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while claiming primary: " + e);
    }
  }

  private boolean checkPrimary() {
    try {
      while (true) {
        try {
          Stat stat = new Stat();
          byte[] data = getZk().getData(getPrimaryPath(), false, stat);
          String primaryId = new String(data);
          isPrimary = primaryId.equals(ephemeralId.toString());
          return true;
        } catch (NoNodeException e) {
          return false;
        } catch (ConnectionLossException ignored) {
        } catch (KeeperException e) {
          throw new RuntimeException("Error checking primary: " + e);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while checking primary: " + e);
    }
  }

  // make sure this node exists, if not create it
  private void assertActiveStateNode() {
    try {
      Stat stat;
      while (true) {
        try {
          stat = getZk().exists(getActiveStatePath(), false);
          break;
        } catch (ConnectionLossException ignored) {

        } catch (KeeperException e) {
          throw new RuntimeException("Error checking active_state exists: " + e);
        }
        if (!checkPrimary() || !isPrimary) {
          throw new IllegalStateException("I am no longer the primary, aborting");
        }
      }
      if (stat == null) {
        while (true) {
          try {
            getZk().create(getActiveStatePath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
            break;
          } catch (NodeExistsException e) {
              break;
          } catch (ConnectionLossException ignored) {

          } catch (KeeperException e) {
            throw new RuntimeException("Error creating empty active_state node: " + e);
          }
          if (!checkPrimary() || !isPrimary) {
            throw new IllegalStateException("I am no longer the primary, aborting");
          }
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted trying to assert active_state node", e);
    }
  }

  private void getActiveState() {
    String activeStateJson;
    try {
      while (true) {
        try {
          activeStateJson = new String(getZk().getData(getActiveStatePath(), false, null));
          break;
        } catch (ConnectionLossException ignored) {

        } catch (KeeperException e) {
          throw new RuntimeException("Error creating getting active_state: " + e);
        }
        if (!checkPrimary() || !isPrimary) {
          throw new IllegalStateException("I am no longer the primary, aborting");
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted getting active state", e);
    }
    try {
      if (!activeStateJson.isEmpty()) {
        activeState = MAPPER.readValue(activeStateJson, NrtActiveState.class);
      } else {
        activeState = null;
      }
    } catch (IOException e) {
      throw new IllegalStateException("Error reading active state json", e);
    }
  }

  public synchronized NrtActiveState getCurrentActiveState() {
    return activeState;
  }

  public synchronized void updateActiveStateWithCopyState(CopyState copyState) {
    // maybe verify this is a newer state?
    String stateString = NrtPublisher.getStateFileName(copyState);
    NrtActiveState newActiveState = new NrtActiveState(stateString);
    this.activeState = newActiveState;
    updateActiveState(newActiveState);
  }

  private final StatCallback updateActiveStateCallback = (rc, path, ctx, stat) -> {
    if (Code.get(rc) == Code.CONNECTIONLOSS) {
      updateActiveState((NrtActiveState) ctx);
    }
  };

  private synchronized void updateActiveState(NrtActiveState newActiveState) {
    if (activeState == newActiveState) {
      String activeStateJson;
      try {
        activeStateJson = MAPPER.writeValueAsString(newActiveState);
      } catch (IOException e) {
        throw new IllegalArgumentException("Error dumping active_state json", e);
      }
      System.out.println("Setting new active_state: " + activeStateJson);
      getZk().setData(getActiveStatePath(), activeStateJson.getBytes(), -1, updateActiveStateCallback, newActiveState);
    } else {
      System.out.println("Skipping update of stale active_state");
    }
  }

  private void initMergeState() {
    String mergeStateJson;
    try {
      mergeStateJson = MAPPER.writeValueAsString(localMergeState);
    } catch (IOException e) {
      throw new RuntimeException("Error dumping merge state json", e);
    }
    try {
      while (true) {
        try {
          String path = getBasePath() + "/" + MERGES_NODE;
          mergeStateNodePath = getZk()
              .create(path, mergeStateJson.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL_SEQUENTIAL);
          break;
        } catch (ConnectionLossException ignored) {
        } catch (KeeperException e) {
          throw new RuntimeException("Error creating initial merge state: " + e);
        }
        if (!checkPrimary() || !isPrimary) {
          throw new IllegalStateException("I am no longer the primary, aborting");
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while trying to create merge state: " + e);
    }
  }

  private final StatCallback updateMergeStateCallback = (rc, path, ctx, stat) -> {
    if (Code.get(rc) == Code.CONNECTIONLOSS) {
      updateMergeState((MergeState) ctx);
    }
  };

  private synchronized void updateMergeState(MergeState newState) {
    if (localMergeState == newState) {
      String mergeStateJson;
      try {
        mergeStateJson = MAPPER.writeValueAsString(newState);
      } catch (IOException e) {
        throw new RuntimeException("Error dumping merge state json", e);
      }
      System.out.println("Updating merge state to: " + mergeStateJson);
      getZk().setData(mergeStateNodePath, mergeStateJson.getBytes(), -1, updateMergeStateCallback, newState);
    } else {
      System.out.println("Merge state is stale, skipping update");
    }
  }

  private final Watcher replicasChangedWatcher = event -> {
    System.out.println("Replicas changed watcher event: " + event);
    if (event.getType() == EventType.NodeChildrenChanged)  {
      getReplicas();
    }
  };

  private final ChildrenCallback getReplicasCallback = (rc, path, ctx, children) -> {
    switch (Code.get(rc)) {
      case CONNECTIONLOSS:
        getReplicas();
        break;
      case OK:
        System.out.println("Got new list of replicas: " + children);
        processNewReplicas(children);
        break;
      default:
        System.out.println("Failed to get replicas: " + KeeperException.create(Code.get(rc), path));
    }
  };

  private void getReplicas() {
    getZk().getChildren(getReplicasBasePath(), replicasChangedWatcher, getReplicasCallback, null);
  }

  private synchronized void processNewReplicas(List<String> newReplicas) {
    System.out.println("Setting new replicas: " + newReplicas);
    Set<String> newReplicasSet = new HashSet<>(newReplicas);
    for (String replica : newReplicasSet) {
      if (!replicaStateMap.containsKey(replica)) {
        System.out.println("Adding replica: " + replica);
        replicaStateMap.put(replica, new ReplicaState(Collections.emptySet()));
        addMergeReplica(replica);
        getReplicaState(replica);
      }
    }
    replicaStateMap.entrySet().removeIf(entry -> {
      if (!newReplicasSet.contains(entry.getKey())) {
        System.out.println("Removing replica: " + entry.getKey());
        removeMergeReplica(entry.getKey());
        return true;
      }
      return false;
    });
  }

  private final Watcher replicaStateWatcher = event -> {
    System.out.println("Processing replica state event: " + event);
    if (event.getType() == EventType.NodeDataChanged) {
      String[] pathSplit = event.getPath().split("/");
      String replica = pathSplit[pathSplit.length - 1];
      System.out.println("Data changed for replica: " + replica);
      getReplicaState(replica);
    }
  };

  private final DataCallback replicaStateCallback = (rc, path, ctx, data, stat) -> {
    String[] pathSplit = path.split("/");
    String replica = pathSplit[pathSplit.length - 1];
    switch (Code.get(rc)) {
      case CONNECTIONLOSS:
        getReplicaState(replica);
        break;
      case OK:
        System.out.println("Got new state for replica: " + replica);
        updateReplicaState(replica, new String(data));
        break;
      case NONODE:
        // this is ok, node membership will be cleaned up by the replicas watcher
        System.out.println("Replica node does not exits: " + replica);
        break;
      default:
        System.out.println("Failed to get replica state: " + KeeperException.create(Code.get(rc), path));
    }
  };

  private void getReplicaState(String replica) {
    String replicaPath = getReplicasBasePath() + "/" + replica;
    getZk().getData(replicaPath, replicaStateWatcher, replicaStateCallback, null);
  }

  private synchronized void updateReplicaState(String replica, String jsonState) {
    System.out.println("Updating replica state: " + replica + ": " + jsonState);
    ReplicaState replicaState;
    try {
      replicaState = MAPPER.readValue(jsonState, ReplicaState.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Error processing replica state json: " + jsonState, e);
    }
    if (replicaStateMap.containsKey(replica)) {
      System.out.println("Updating local state for replica: " + replica);
      replicaStateMap.put(replica, replicaState);
    } else {
      System.out.println("Not updating local state for unknown replica: " + replica);
    }
  }

  public synchronized void addMergeReplica(String replica) {
    MergeState newMergeState = new MergeState(localMergeState);
    Map<String, NrtFileMetaData> initialMerges = new HashMap<>();
    localMergeState.activeMerges.forEach(initialMerges::put);
    newMergeState.replicas.put(replica, new ReplicaMergeState(initialMerges));
    localMergeState = newMergeState;
    updateMergeState(newMergeState);
  }

  public synchronized void removeMergeReplica(String replica) {
    MergeState newMergeState = new MergeState(localMergeState);
    newMergeState.replicas.remove(replica);
    localMergeState = newMergeState;
    updateMergeState(newMergeState);
  }

  public synchronized void addMergeFiles(Map<String, FileMetaData> files) {
    MergeState newMergeState = new MergeState(localMergeState);
    for (Map.Entry<String, FileMetaData> entry : files.entrySet()) {
      newMergeState.activeMerges.put(entry.getKey(), new NrtFileMetaData(entry.getValue()));
    }
    localMergeState = newMergeState;
    updateMergeState(newMergeState);
  }

  public synchronized boolean areMergesCompleted(Map<String, FileMetaData> files) {
    for (Map.Entry<String, ReplicaState> replicaEntry : replicaStateMap.entrySet()) {
      for (Map.Entry<String, FileMetaData> entry : files.entrySet()) {
        if (!replicaEntry.getValue().warmedMerges.contains(entry.getKey())) {
          System.out.println("Replica " + replicaEntry.getKey() + " has not warmed file: " + entry.getKey());
          return false;
        }
      }
    }
    System.out.println("Removing pending merges");
    MergeState newMergeState = new MergeState(localMergeState);
    for (Map.Entry<String, FileMetaData> entry : files.entrySet()) {
      newMergeState.activeMerges.remove(entry.getKey());
    }
    localMergeState = newMergeState;
    updateMergeState(newMergeState);
    return true;
  }
}
