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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;

public final class NrtPointState {

  public Map<String, NrtFileMetaData> files;
  public long version;
  public long gen;
  public byte[] infosBytes;
  public long primaryGen;

  public NrtPointState() {}

  public NrtPointState(CopyState copyState) {
    version = copyState.version;
    gen = copyState.gen;
    infosBytes = copyState.infosBytes;
    primaryGen = copyState.primaryGen;
    files =
        copyState.files.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, v -> new NrtFileMetaData(v.getValue())));
  }

  @JsonIgnore
  public CopyState getCopyState() {
    Map<String, FileMetaData> luceneFiles =
        files.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, v -> v.getValue().toFileMetaData()));
    return new CopyState(
        luceneFiles, version, gen, infosBytes, Collections.emptySet(), primaryGen, null);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof NrtPointState) {
      NrtPointState other = (NrtPointState) o;
      return this.primaryGen == other.primaryGen
          && this.gen == other.gen
          && this.version == other.version;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("NrtPointState(%d, %d, %d)", primaryGen, gen, version);
  }
}
