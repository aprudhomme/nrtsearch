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
package com.yelp.nrtsearch.server.luceneserver.nrt.s3;

import com.amazonaws.services.s3.model.S3Object;
import com.yelp.nrtsearch.server.luceneserver.nrt.DataFileNameUtils;
import com.yelp.nrtsearch.server.luceneserver.nrt.ReplicaDataManager;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.ActiveState;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;

public class ReplicaS3DataManager extends S3DataManager implements ReplicaDataManager {

  private Map<String, FileMetaData> initialFiles;

  public ReplicaS3DataManager(
      String indexName, int shardOrd, String serviceName, Directory directory) throws IOException {
    super(indexName, shardOrd, serviceName, directory);
  }

  @Override
  public void syncInitialActiveState(ActiveState activeState) throws IOException {
    NrtPointState pointState = fetchActiveStateNrtPoint(activeState);
    syncInitialNrtPoint(pointState);
    if (pointState != null) {
      initialFiles =
          pointState.files.entrySet().stream()
              .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    } else {
      initialFiles = Collections.emptyMap();
    }
  }

  @Override
  public Map<String, FileMetaData> getInitialFiles() throws IOException {
    return initialFiles;
  }

  @Override
  public DataInput getFileDataInput(String fileName, NrtFileMetaData metaData) throws IOException {
    String fileKey = getDataBasePath() + DataFileNameUtils.getDataFileName(metaData, fileName);
    S3Object s3Object = getS3Client().getObject(getBaseBucket(), fileKey);
    return new S3DataInput(s3Object);
  }

  @Override
  public NrtPointState getNrtPointForActiveState(ActiveState activeState) throws IOException {
    return fetchActiveStateNrtPoint(activeState);
  }
}
