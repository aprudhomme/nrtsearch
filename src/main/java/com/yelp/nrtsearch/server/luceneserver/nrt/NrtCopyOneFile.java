package com.yelp.nrtsearch.server.luceneserver.nrt;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.replicator.nrt.CopyOneFile;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.store.DataInput;

public class NrtCopyOneFile extends CopyOneFile {

  private DataInput dataInput;

  public NrtCopyOneFile(DataInput in,
      ReplicaNode dest, String name,
      FileMetaData metaData, byte[] buffer) throws IOException {
    super(in, dest, name, metaData, buffer);
    dataInput = in;
  }

  public NrtCopyOneFile(CopyOneFile other, DataInput in) {
    super(other, in);
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (dataInput instanceof Closeable) {
      ((Closeable) dataInput).close();
    }
  }
}
