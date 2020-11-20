package com.yelp.nrtsearch.server.luceneserver.nrt;

import java.util.Map;

public class MergeState {
  public String primaryId;
  public long primaryGen;
  public Map<String, ReplicaMergeState> replicas;
  public Map<String, NrtFileMetaData> activeMerges;

  public MergeState() {

  }

  public MergeState(String primaryId, long primaryGen, Map<String, ReplicaMergeState> replicas, Map<String, NrtFileMetaData> activeMerges) {
    this.primaryId = primaryId;
    this.primaryGen = primaryGen;
    this.replicas = replicas;
    this.activeMerges = activeMerges;
  }

  public MergeState(MergeState other) {
    this.primaryId = other.primaryId;
    this.primaryGen = other.primaryGen;
    this.replicas = other.replicas;
    this.activeMerges = other.activeMerges;
  }

  public static class ReplicaMergeState {
    public Map<String, NrtFileMetaData> initialPendingMerges;

    public ReplicaMergeState() {}

    public ReplicaMergeState(Map<String, NrtFileMetaData> initialPendingMerges) {
      this.initialPendingMerges = initialPendingMerges;
    }
  }
}
