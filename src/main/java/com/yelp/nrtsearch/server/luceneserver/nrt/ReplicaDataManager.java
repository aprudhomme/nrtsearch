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

import com.yelp.nrtsearch.server.luceneserver.nrt.state.ActiveState;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.store.DataInput;

public interface ReplicaDataManager extends Closeable {
  void syncInitialActiveState(ActiveState activeState) throws IOException;

  Map<String, FileMetaData> getInitialFiles() throws IOException;

  DataInput getFileDataInput(String fileName, NrtFileMetaData metaData) throws IOException;

  NrtPointState getNrtPointForActiveState(ActiveState activeState) throws IOException;
}
