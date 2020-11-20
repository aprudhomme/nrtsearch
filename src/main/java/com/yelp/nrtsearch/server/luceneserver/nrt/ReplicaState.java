package com.yelp.nrtsearch.server.luceneserver.nrt;

import java.util.Set;

public class ReplicaState {

  public Set<String> warmedMerges;

  public ReplicaState() {}

  public ReplicaState(Set<String> warmedMerges) {
    this.warmedMerges = warmedMerges;
  }

  public ReplicaState(ReplicaState other) {
    this.warmedMerges = other.warmedMerges;
  }
}
