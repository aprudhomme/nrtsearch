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
package com.yelp.nrtsearch.server.luceneserver.nrt.state;

import java.util.Map;

public class MergeState {
  public String primaryId;
  public long primaryGen;
  public Map<String, ReplicaMergeState> replicas;
  public Map<String, NrtFileMetaData> activeMerges;

  public MergeState() {}

  public MergeState(
      String primaryId,
      long primaryGen,
      Map<String, ReplicaMergeState> replicas,
      Map<String, NrtFileMetaData> activeMerges) {
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

  public static class InitialReplicaMergeState {
    public ReplicaMergeState replicaMergeState;
    public long primaryGen;

    public InitialReplicaMergeState() {}

    public InitialReplicaMergeState(ReplicaMergeState replicaMergeState, long primaryGen) {
      this.replicaMergeState = replicaMergeState;
      this.primaryGen = primaryGen;
    }
  }
}
