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

import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.yelp.nrtsearch.server.luceneserver.nrt.PrimaryDataManager;
import com.yelp.nrtsearch.server.luceneserver.nrt.StateFileNameUtils;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.ActiveState;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

public class PrimaryS3DataManager extends S3DataManager implements PrimaryDataManager {
  private static final int SEND_BUFFER_SIZE = 5 * 1024 * 1024;
  private final byte[] sendBuffer = new byte[SEND_BUFFER_SIZE];
  private final InputStream sendStream = new ByteArrayInputStream(sendBuffer);

  private Map<String, FileMetaData> lastPublishedFiles;
  private NrtPointState lastPublishedNrtPoint;

  private final Map<String, FileMetaData> pendingMergeFiles = new HashMap<>();

  public PrimaryS3DataManager(
      String indexName, int shardOrd, String serviceName, Directory directory) throws IOException {
    super(indexName, shardOrd, serviceName, directory);
  }

  @Override
  public void syncInitialActiveState(ActiveState activeState) throws IOException {
    NrtPointState pointState = fetchActiveStateNrtPoint(activeState);
    syncInitialNrtPoint(pointState);
    if (pointState != null) {
      lastPublishedFiles = pointState.getCopyState().files;
      lastPublishedNrtPoint = pointState;
    } else {
      lastPublishedFiles = Collections.emptyMap();
      lastPublishedNrtPoint = null;
    }
  }

  @Override
  public synchronized void publishNrtPoint(CopyState copyState) throws IOException {
    NrtPointState pointState = new NrtPointState(copyState);
    if (pointState.equals(lastPublishedNrtPoint)) {
      System.out.println("Already published state, skipping: " + pointState);
    }

    String stateStr = MAPPER.writeValueAsString(pointState);
    System.out.println("State: " + stateStr);

    // upload state
    getS3Client()
        .putObject(
            getBaseBucket(),
            getStateBasePath() + StateFileNameUtils.getStateFileName(pointState),
            stateStr);

    List<String> filesToPublish = getFilesToPublish(copyState);
    System.out.println("Files to upload: " + filesToPublish);
    // upload files
    for (String file : filesToPublish) {
      publishFile(file);
    }

    lastPublishedFiles = copyState.files;
    lastPublishedNrtPoint = pointState;

    synchronized (pendingMergeFiles) {
      for (String file : filesToPublish) {
        pendingMergeFiles.remove(file);
      }
    }
  }

  private List<String> getFilesToPublish(CopyState copyState) {
    List<String> copyList = new ArrayList<>();
    synchronized (pendingMergeFiles) {
      for (Map.Entry<String, FileMetaData> entry : copyState.files.entrySet()) {
        if (!lastPublishedFiles.containsKey(entry.getKey())) {
          if (!pendingMergeFiles.containsKey(entry.getKey())) {
            copyList.add(entry.getKey());
          }
        }
      }
    }
    return copyList;
  }

  private synchronized void publishFile(String file) throws IOException {
    String fileKey = getDataBasePath() + file;
    try (IndexInput indexInput = getDirectory().openInput(file, IOContext.DEFAULT)) {
      long fileLength = indexInput.length();

      List<PartETag> partETags = new ArrayList<>();

      // Initiate the multipart upload.
      InitiateMultipartUploadRequest initRequest =
          new InitiateMultipartUploadRequest(getBaseBucket(), fileKey);
      InitiateMultipartUploadResult initResponse =
          getS3Client().initiateMultipartUpload(initRequest);

      long filePosition = 0;
      for (int i = 1; filePosition < fileLength; i++) {
        // Because the last part could be less than 5 MB, adjust the part size as needed.
        long partSize = Math.min(SEND_BUFFER_SIZE, (fileLength - filePosition));

        indexInput.seek(filePosition);
        indexInput.readBytes(sendBuffer, 0, (int) partSize);

        sendStream.reset();

        // Create the request to upload a part.
        UploadPartRequest uploadRequest =
            new UploadPartRequest()
                .withBucketName(getBaseBucket())
                .withKey(fileKey)
                .withUploadId(initResponse.getUploadId())
                .withPartNumber(i)
                .withPartSize(partSize)
                .withInputStream(sendStream);

        // Upload the part and add the response's ETag to our list.
        UploadPartResult uploadResult = getS3Client().uploadPart(uploadRequest);
        partETags.add(uploadResult.getPartETag());

        filePosition += partSize;
      }

      // Complete the multipart upload.
      CompleteMultipartUploadRequest compRequest =
          new CompleteMultipartUploadRequest(
              getBaseBucket(), fileKey, initResponse.getUploadId(), partETags);
      getS3Client().completeMultipartUpload(compRequest);
    }
    System.out.println("Finished file upload: " + fileKey);
  }

  @Override
  public void publishMergeFiles(Map<String, FileMetaData> files) throws IOException {
    for (Map.Entry<String, FileMetaData> entry : files.entrySet()) {
      System.out.println("Publishing merge file: " + entry.getKey());
      publishFile(entry.getKey());
    }
    synchronized (pendingMergeFiles) {
      pendingMergeFiles.putAll(files);
    }
  }
}
