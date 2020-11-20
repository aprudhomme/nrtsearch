package com.yelp.nrtsearch.server.luceneserver.nrt;

import com.yelp.nrtsearch.server.luceneserver.NRTReplicaNode;
import com.yelp.nrtsearch.server.luceneserver.nrt.MergeState.ReplicaMergeState;
import com.yelp.nrtsearch.server.luceneserver.nrt.NrtPublisher.StateInfo;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;

public class ReplicaStateManager extends StateManager {

  Set<String> warmedSegments = new HashSet<>();

  ReplicaState currentState;

  String lastMergeStateNode = "";
  MergeState mergeState;

  private NRTReplicaNode replicaNode;
  private NrtActiveState activeState;

  public ReplicaStateManager(String indexName, int shardOrd) throws IOException {
    super(indexName, shardOrd);

    currentState = new ReplicaState(Collections.emptySet());

    // TODO make synchronous
    initReplicaState();

    // get the current state value (blocking), then set up an async watch
    getActiveStateSync();

  }

  public void startMergeHandling(NRTReplicaNode replicaNode) {
    this.replicaNode = replicaNode;
    getShardState();
  }

  public void startStateUpdates() {
    getActiveState();
  }

  public synchronized void getActiveStateSync() {
    String activeStateJson;
    try {
      while (true) {
        try {
          activeStateJson = new String(getZk().getData(getActiveStatePath(), false, null));
          break;
        } catch (ConnectionLossException ignored) {

        } catch (NoNodeException e) {
          throw new IllegalStateException("There is no active_state node", e);
        } catch (KeeperException e) {
          throw new RuntimeException("Error creating getting active_state: " + e);
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

  private final Watcher getActiveStateWatcher = event -> {
    System.out.println("Get active state event: " + event);
    if (event.getType() == EventType.NodeDataChanged) {
      getActiveState();
    }
  };

  private final DataCallback getActiveStateCallback = (rc, path, ctx, data, stat) -> {
    switch (Code.get(rc)) {
      case CONNECTIONLOSS:
        getActiveState();
        break;
      case OK:
        setActiveState(new String(data));
        break;
      default:
        System.out.println("Failed to get active_state: " + KeeperException.create(Code.get(rc), path));
    }
  };

  private void getActiveState() {
    getZk().getData(getActiveStatePath(), getActiveStateWatcher, getActiveStateCallback, null);
  }

  private synchronized void setActiveState(String activeStateJson) {
    System.out.println("Setting new active state: " + activeStateJson);
    try {
      if (!activeStateJson.isEmpty()) {
        activeState = MAPPER.readValue(activeStateJson, NrtActiveState.class);
      } else {
        activeState = null;
      }
    } catch (IOException e) {
      throw new IllegalStateException("Error reading active state json", e);
    }
    if (activeState != null) {
      StateInfo info = NrtPublisher.getStateInfoFromFileName(activeState.stateFile);
      System.out.println("Sending new nrtpoint: primary: " + info.primaryGen + ", gen: " + info.gen + ", version: " + info.version);
      try {
        replicaNode.newNRTPoint(info.primaryGen, info.version);
      } catch (IOException e) {
        System.out.println("Got exception when creating new NRT Point: " + e);
      }
    }
  }

  public synchronized NrtActiveState getCurrentActiveState() {
    return activeState;
  }

  private final StringCallback initStateCallback = (rc, path, ctx, name) -> {
    switch (Code.get(rc)) {
      case CONNECTIONLOSS:
        initReplicaState();
        break;
      case OK:
        System.out.println("Registered initial state");
        break;
      case NODEEXISTS:
        System.out.println("State is already registered?");
        break;
      default:
        System.out.println("Error registering replica state: " + KeeperException.create(Code.get(rc), path));
    }
  };

  public void initReplicaState() {
    String stateJson;
    try {
      stateJson = MAPPER.writeValueAsString(currentState);
    } catch (IOException e) {
      throw new IllegalStateException("Error converting replica state to json: " + e);
    }
    String replicaPath = getReplicasBasePath() + "/" + ephemeralId.toString();
    getZk().create(replicaPath, stateJson.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL, initStateCallback, null);
  }

  public synchronized void addWarmedMerges(Set<String> files) {
    System.out.println("Adding warmed merges: " + files);
    warmedSegments.addAll(files);
    ReplicaState newReplicaState = new ReplicaState(currentState);
    newReplicaState.warmedMerges = warmedSegments;
    currentState = newReplicaState;
    updateReplicaState(newReplicaState);
  }

  private final StatCallback updateStateCallback = (rc, path, ctx, stat) -> {
    if (Code.get(rc) == Code.CONNECTIONLOSS) {
      updateReplicaState((ReplicaState) ctx);
    }
  };

  private synchronized void updateReplicaState(ReplicaState newReplicaState) {
    // only update if we are holding the current reference,
    // there may be a more current version in event of a connection loss
    if (currentState == newReplicaState) {
      String replicaPath = getReplicasBasePath() + "/" + ephemeralId.toString();
      String stateJson;
      try {
        stateJson = MAPPER.writeValueAsString(newReplicaState);
      } catch (IOException e) {
        throw new IllegalStateException("Error converting replica state to json: " + e);
      }
      System.out.println("Updating replica state to: " + stateJson);
      getZk().setData(replicaPath, stateJson.getBytes(), -1, updateStateCallback, newReplicaState);
    }
  }

  private final Watcher shardStateWatcher = event -> {
    System.out.println("Shard state nodes changed watcher event: " + event);
    if (event.getType() == EventType.NodeChildrenChanged)  {
      getShardState();
    }
  };

  private final ChildrenCallback getShardStateCallback = (rc, path, ctx, children) -> {
    switch (Code.get(rc)) {
      case CONNECTIONLOSS:
        getShardState();
        break;
      case OK:
        System.out.println("Got new list of state nodes: " + children);
        processStateChildren(children);
        break;
      default:
        System.out.println("Failed to get shard state children: " + KeeperException.create(Code.get(rc), path));
    }
  };

  private void getShardState() {
    getZk().getChildren(getBasePath(), shardStateWatcher, getShardStateCallback, null);
  }

  private void processStateChildren(List<String> children) {
    System.out.println("Processing state children: " + children);
    String mergeStateNode = "";
    for (String child : children) {
      if (child.startsWith(MERGES_NODE)) {
        if (!mergeStateNode.isEmpty()) {
          if (mergeStateNode.compareTo(child) < 0) {
            mergeStateNode = child;
          }
        } else {
          mergeStateNode = child;
        }
      }
    }
    System.out.println("Got new merge state node: " + mergeStateNode);
    if (!mergeStateNode.equals(lastMergeStateNode)) {
      if (lastMergeStateNode != null) {
        resetLocalMergeState(mergeStateNode);
      }
      if (!mergeStateNode.isEmpty()) {
        getMergeState(mergeStateNode);
      }
    }
  }

  private synchronized void resetLocalMergeState(String newNode) {
    System.out.println("Resetting local merge state, new node: " + newNode);
    updateMergeJobs(mergeState, null);
    mergeState = null;
    lastMergeStateNode = newNode;
  }

  private final Watcher mergeStateWatcher = event -> {
    System.out.println("Processing merge state event: " + event);
    if (event.getType() == EventType.NodeDataChanged) {
      String[] pathSplit = event.getPath().split("/");
      String node = pathSplit[pathSplit.length - 1];
      System.out.println("Data changed for state node: " + node);
      getMergeState(node);
    }
  };

  private final DataCallback mergeStateCallback = (rc, path, ctx, data, stat) -> {
    String node = (String) ctx;
    switch (Code.get(rc)) {
      case CONNECTIONLOSS:
        getMergeState(node);
        break;
      case OK:
        System.out.println("Got new merge state from node: " + node);
        updateMergeState(node, new String(data));
        break;
      case NONODE:
        System.out.println("Merge state node does not exits: " + node);
        break;
      default:
        System.out.println("Failed to get merge state: " + KeeperException.create(Code.get(rc), path));
    }
  };

  private void getMergeState(String currentNode) {
    String path = getBasePath() + "/" + currentNode;
    getZk().getData(path, mergeStateWatcher, mergeStateCallback, currentNode);
  }

  private synchronized void updateMergeState(String node, String stateJson) {
    if (node.equals(lastMergeStateNode)) {
      System.out.println("New merge state, " + node + ": " + stateJson);
      MergeState newMergeState;
      try {
        newMergeState = MAPPER.readValue(stateJson, MergeState.class);
      } catch (IOException e) {
        throw new RuntimeException("Error reading merge state json", e);
      }
      updateMergeJobs(mergeState, newMergeState);
      mergeState = newMergeState;
    } else {
      System.out.println("Skipping stale merge state for node: " + node);
    }
  }

  private synchronized void updateMergeJobs(MergeState oldState, MergeState newState) {
    if (oldState == newState) {
      return;
    }

    if (newState == null) {
      warmedSegments.clear();
      ReplicaState newReplicaState = new ReplicaState(currentState);
      newReplicaState.warmedMerges = warmedSegments;
      currentState = newReplicaState;
      updateReplicaState(newReplicaState);
    } else if (oldState == null) {
      replicaNode.startMergeTask(newState.activeMerges, newState.primaryGen);
    } else {

      if (oldState.primaryGen != newState.primaryGen || !oldState.primaryId
          .equals(newState.primaryId)) {
        // clean up an pending local state
      }
      // find new merge files to warm
      Map<String, NrtFileMetaData> newMergeFiles = new HashMap<>();
      for (Map.Entry<String, NrtFileMetaData> entry : newState.activeMerges.entrySet()) {
        if (!oldState.activeMerges.containsKey(entry.getKey())) {
          newMergeFiles.put(entry.getKey(), entry.getValue());
        }
      }
      replicaNode.startMergeTask(newMergeFiles, newState.primaryGen);

      warmedSegments.removeIf(s -> {
        if (!newState.activeMerges.containsKey(s)) {
          System.out.println("Removing completed merge from warmedMergers: " + s);
          return true;
        }
        return false;
      });
      ReplicaState newReplicaState = new ReplicaState(currentState);
      newReplicaState.warmedMerges = warmedSegments;
      currentState = newReplicaState;
      updateReplicaState(newReplicaState);
    }
  }

  public static class InitialMergeState {
    public ReplicaMergeState replicaMergeState;
    public long primaryGen;
  }

  public InitialMergeState blockForInitialMergeState() {
    int allowedAbsentCount = 5;
    int absentCount = 0;
    while (absentCount < allowedAbsentCount) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) { }
      ReplicaMergeStateAndPresent maybeState = getMyMergeStateAndPresent();
      if (!maybeState.present) {
        absentCount++;
      }
      if (maybeState.state != null) {
        return maybeState.state;
      }
      System.out.println("Waiting for initial merge state");
    }
    return null;
  }

  private static class ReplicaMergeStateAndPresent {
    boolean present;
    InitialMergeState state;
  }

  private synchronized ReplicaMergeStateAndPresent getMyMergeStateAndPresent() {
    ReplicaMergeStateAndPresent ret = new ReplicaMergeStateAndPresent();
    if (mergeState == null) {
      ret.present = false;
      ret.state = null;
    } else {
      ret.present = true;
      ReplicaMergeState replicaMergeState = mergeState.replicas.get(ephemeralId.toString());
      if (replicaMergeState != null) {
        ret.state = new InitialMergeState();
        ret.state.replicaMergeState = replicaMergeState;
        ret.state.primaryGen = mergeState.primaryGen;
      }
    }
    return ret;
  }
}
