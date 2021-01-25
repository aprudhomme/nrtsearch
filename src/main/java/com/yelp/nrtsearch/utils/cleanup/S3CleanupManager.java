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
package com.yelp.nrtsearch.utils.cleanup;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.yelp.nrtsearch.server.luceneserver.nrt.DataFileNameUtils;
import com.yelp.nrtsearch.server.luceneserver.nrt.StateFileNameUtils;
import com.yelp.nrtsearch.server.luceneserver.nrt.s3.S3DataManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.ToLongFunction;

public class S3CleanupManager extends S3DataManager {
  private static final int DELETE_BATCH_SIZE = 1000;

  public S3CleanupManager(String indexName, int shardOrd, String serviceName) throws IOException {
    super(indexName, shardOrd, serviceName, null);
  }

  public void verifyDataFilesExists(Set<String> files) {
    verifyFilesExist(files, getDataBasePath());
    System.out.println("All referenced data files exist");
  }

  public void verifyStateFilesExists(Set<String> files) {
    verifyFilesExist(files, getStateBasePath());
    System.out.println("All referenced state files exist");
  }

  private void verifyFilesExist(Set<String> files, String baseKey) {
    for (String file : files) {
      String key = baseKey + file;
      if (!getS3Client().doesObjectExist(getBaseBucket(), key)) {
        throw new IllegalArgumentException("File does not exist: " + key);
      }
    }
  }

  public void cleanupDataFiles(Set<String> files, long maxTimestamp) {
    System.out.println("Started data file cleanup");
    cleanupFiles(
        files,
        maxTimestamp,
        getDataBasePath(),
        name -> DataFileNameUtils.getDataInfoFromFileName(name).timestamp);
    System.out.println("Finished data file cleanup");
  }

  public void cleanupStateFiles(Set<String> files, long maxTimestamp) {
    System.out.println("Started state file cleanup");
    cleanupFiles(
        files,
        maxTimestamp,
        getStateBasePath(),
        name -> StateFileNameUtils.getStateInfoFromFileName(name).timestamp);
    System.out.println("Finished state file cleanup");
  }

  private void cleanupFiles(
      Set<String> files, long maxTimestamp, String baseKey, ToLongFunction<String> tsExtractor) {
    ListObjectsV2Request req =
        new ListObjectsV2Request().withBucketName(getBaseBucket()).withPrefix(baseKey);
    ListObjectsV2Result result;

    List<String> deleteList = new ArrayList<>(DELETE_BATCH_SIZE);

    listLoop:
    do {
      result = getS3Client().listObjectsV2(req);

      for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        String objFileName = objectSummary.getKey().split(baseKey)[1];
        long timestamp = tsExtractor.applyAsLong(objFileName);
        if (!files.contains(objFileName) && timestamp <= maxTimestamp) {
          System.out.println("Deleting object - name: " + objFileName + ", ts: " + timestamp);
          deleteList.add(objectSummary.getKey());
          if (deleteList.size() == DELETE_BATCH_SIZE) {
            deleteObjects(deleteList);
            deleteList.clear();
          }
        } else if (timestamp > maxTimestamp) {
          System.out.println("Past max ts - name: " + objFileName + ", ts: " + timestamp);
          break listLoop;
        } else {
          System.out.println("File is referenced - name: " + objFileName + ", ts: " + timestamp);
        }
      }
      String token = result.getNextContinuationToken();
      req.setContinuationToken(token);
    } while (result.isTruncated());

    if (!deleteList.isEmpty()) {
      deleteObjects(deleteList);
    }
  }

  private void deleteObjects(List<String> keys) {
    System.out.println("Batch deleting objects, size: " + keys.size());
    DeleteObjectsRequest multiObjectDeleteRequest =
        new DeleteObjectsRequest(getBaseBucket())
            .withKeys(keys.toArray(new String[0]))
            .withQuiet(true);
    getS3Client().deleteObjects(multiObjectDeleteRequest);
  }
}
