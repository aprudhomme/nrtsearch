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
package com.yelp.nrtsearch.server.luceneserver.nrt;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.replicator.nrt.CopyOneFile;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.store.DataInput;

public class NrtCopyOneFile extends CopyOneFile {

  private DataInput dataInput;

  public NrtCopyOneFile(
      DataInput in, ReplicaNode dest, String name, FileMetaData metaData, byte[] buffer)
      throws IOException {
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
