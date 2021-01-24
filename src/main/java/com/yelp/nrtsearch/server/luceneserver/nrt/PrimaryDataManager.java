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
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.replicator.nrt.CopyState;

public interface PrimaryDataManager extends Closeable {
  void syncInitialActiveState(ActiveState activeState) throws IOException;

  void publishNrtPoint(CopyState copyState, long timestamp) throws IOException;

  void publishMergeFiles(Map<String, NrtFileMetaData> files) throws IOException;
}
