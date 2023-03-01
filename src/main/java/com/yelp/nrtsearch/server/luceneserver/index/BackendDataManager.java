package com.yelp.nrtsearch.server.luceneserver.index;

import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.index.backend.DataBackend;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.Node;
import org.apache.lucene.replicator.nrt.PrimaryNode;
import org.apache.lucene.replicator.nrt.ReplicaNode;

public class BackendDataManager implements IndexDataManager {
  private CopyState lastState;
  private final String indexName;
  private final String indexId;
  private final String indexUniqueName;
  private final DataBackend dataBackend;
  private final GlobalState globalState;
  private final Path indexDataRoot;

  public BackendDataManager(String indexName, String id, IndexStateManager stateManager, DataBackend dataBackend, GlobalState globalState) {
    this.indexName = indexName;
    this.indexId = id;
    this.indexUniqueName = BackendGlobalState.getUniqueIndexName(indexName, indexId);
    this.dataBackend = dataBackend;
    this.globalState = globalState;

    Path indexRoot = stateManager.getCurrent().getRootDir();
    indexDataRoot = indexRoot.resolve(ShardState.getShardDirectoryName(0)).resolve(ShardState.INDEX_DATA_DIR_NAME);
  }

  @Override
  public void setInitialCopyState(CopyState copyState) {
    lastState = copyState;
  }

  @Override
  public void uploadIndexData(CopyState copyState) {
    List<String> newIndexFiles = getNewFiles(lastState, copyState);
    // filter merges

    // upload files
    System.out.println("Upload files: " + newIndexFiles);

    // upload and bless copy state

    lastState = copyState;
  }

  static List<String> getNewFiles(CopyState previous, CopyState current) {
    Objects.requireNonNull(current);
    if (previous == null) {
      return new ArrayList<>(current.files.keySet());
    }
    List<String> newFilesList = new ArrayList<>();
    for (Map.Entry<String, FileMetaData> entry : current.files.entrySet()) {
      FileMetaData previousMetaData = previous.files.get(entry.getKey());
      if (previousMetaData == null) {
        newFilesList.add(entry.getKey());
      } else if (!fileMetaDataEquals(entry.getValue(), previousMetaData)) {
        throw new IllegalArgumentException("Index file " + entry.getKey() + " has changed");
      }
    }
    return newFilesList;
  }

  static boolean fileMetaDataEquals(FileMetaData first, FileMetaData second) {
    return first.length == second.length && first.checksum == second.checksum && Arrays.equals(first.header, second.header) && Arrays.equals(first.footer, second.footer);
  }

  @Override
  public void close() throws IOException {

  }
}
