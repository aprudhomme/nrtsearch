package com.yelp.nrtsearch.server.luceneserver.index;

import java.io.Closeable;
import org.apache.lucene.replicator.nrt.CopyState;

public interface IndexDataManager extends Closeable {
  void setInitialCopyState(CopyState copyState);
  void uploadIndexData(CopyState copyState);
}
