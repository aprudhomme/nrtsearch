package com.yelp.nrtsearch.server.luceneserver.nrt;


import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;

public class NrtPointState {

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
    files = copyState.files.entrySet().stream().collect(Collectors.toMap(Entry::getKey, v -> new NrtFileMetaData(v.getValue())));
  }

  @JsonIgnore
  public CopyState getCopyState() {
    Map<String, FileMetaData> luceneFiles = files.entrySet().stream().collect(Collectors.toMap(Entry::getKey, v -> v.getValue().toFileMetaData()));
    return new CopyState(luceneFiles, version, gen, infosBytes, Collections.emptySet(), primaryGen, null);
  }
}
