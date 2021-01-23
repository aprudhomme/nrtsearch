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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.yelp.nrtsearch.server.luceneserver.nrt.PrimaryDataManager;
import com.yelp.nrtsearch.server.luceneserver.nrt.StateFileNameUtils;
import com.yelp.nrtsearch.server.luceneserver.nrt.s3.S3ThreadFactory.S3Thread;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.ActiveState;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

public class PrimaryS3DataManager extends S3DataManager implements PrimaryDataManager {
  private final ThreadPoolExecutor mergeThreadPool;
  private final ThreadPoolExecutor publishThreadPool;

  private Map<String, FileMetaData> lastPublishedFiles;
  private NrtPointState lastPublishedNrtPoint;

  private final Map<String, FileMetaData> pendingMergeFiles = new HashMap<>();

  public PrimaryS3DataManager(
      String indexName, int shardOrd, String serviceName, Directory directory) throws IOException {
    super(indexName, shardOrd, serviceName, directory);
    mergeThreadPool =
        (ThreadPoolExecutor)
            Executors.newFixedThreadPool(4, new S3ThreadFactory(this::getS3Client));
    publishThreadPool =
        (ThreadPoolExecutor)
            Executors.newFixedThreadPool(10, new S3ThreadFactory(this::getS3Client));
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

    Map<String, FileMetaData> filesToPublish = getFilesToPublish(copyState);
    System.out.println("Files to upload: " + filesToPublish.keySet());
    // upload files
    parallelPublishFiles(filesToPublish, publishThreadPool);

    lastPublishedFiles = copyState.files;
    lastPublishedNrtPoint = pointState;

    synchronized (pendingMergeFiles) {
      for (String file : filesToPublish.keySet()) {
        pendingMergeFiles.remove(file);
      }
    }
  }

  private Map<String, FileMetaData> getFilesToPublish(CopyState copyState) {
    Map<String, FileMetaData> copyMap = new HashMap<>();
    synchronized (pendingMergeFiles) {
      for (Map.Entry<String, FileMetaData> entry : copyState.files.entrySet()) {
        if (!lastPublishedFiles.containsKey(entry.getKey())) {
          if (!pendingMergeFiles.containsKey(entry.getKey())) {
            copyMap.put(entry.getKey(), entry.getValue());
          }
        }
      }
    }
    return copyMap;
  }

  @Override
  public void publishMergeFiles(Map<String, FileMetaData> files) throws IOException {
    System.out.println("Publishing merge files: " + files.keySet());
    parallelPublishFiles(files, mergeThreadPool);
    synchronized (pendingMergeFiles) {
      pendingMergeFiles.putAll(files);
    }
  }

  private void parallelPublishFiles(Map<String, FileMetaData> files, ThreadPoolExecutor threadPool)
      throws IOException {
    List<PublishJob> jobList = new ArrayList<>(files.size());
    try {
      for (Map.Entry<String, FileMetaData> entry : files.entrySet()) {
        String key = getDataBasePath() + entry.getKey();
        IndexInput fileInput = getDirectory().openInput(entry.getKey(), IOContext.DEFAULT);
        PublishJob job = new PublishJob(key, entry.getValue().length, fileInput);
        jobList.add(job);
        job.startJob(threadPool);
      }
      for (PublishJob job : jobList) {
        job.waitUntilDone();
      }
    } finally {
      IOUtils.closeWhileHandlingException(jobList);
    }
  }

  private class PublishJob implements Closeable {
    private static final long FILE_5MB = 5 * 1024 * 1024;
    private static final long FILE_50MB = 50 * 1024 * 1024;
    private static final long FILE_500MB = 500 * 1024 * 1024;
    private static final long FILE_5GB = 5L * 1024 * 1024 * 1024;
    private static final long FILE_50GB = 50L * 1024 * 1024 * 1024;

    private final String key;
    private final long length;
    private final long chunkSize;
    private final int numChunks;
    private final IndexInput input;

    private List<PartETag> tagList;
    private String uploadId;

    private boolean done = false;
    private RuntimeException error = null;

    PublishJob(String key, long length, IndexInput input) {
      this.key = key;
      this.length = length;
      this.input = input;
      chunkSize = computeChunkSize(length);
      int chunks = (int) (length / chunkSize);
      if ((length % chunkSize) > 0) {
        chunks++;
      }
      if (chunks > 10000) {
        throw new IllegalArgumentException(
            "Multipart upload limited to 10000 parts, needs " + chunks);
      }
      numChunks = chunks;
    }

    void startJob(ThreadPoolExecutor executor) throws IOException {
      System.out.println(
          "Start job for key: " + key + ", chunks: " + numChunks + ", chunkSize: " + chunkSize);
      if (numChunks == 1) {
        executor.execute(
            new PublishTask(0, length, key, new IndexInputStream(input, 0, length), ""));
      } else {
        tagList = new ArrayList<>(numChunks);
        // Initiate the multipart upload.
        InitiateMultipartUploadRequest initRequest =
            new InitiateMultipartUploadRequest(getBaseBucket(), key);
        InitiateMultipartUploadResult initResponse =
            getS3Client().initiateMultipartUpload(initRequest);
        uploadId = initResponse.getUploadId();

        long filePosition = 0;
        for (int i = 1; filePosition < length; i++) {
          long partSize = Math.min(chunkSize, (length - filePosition));
          executor.execute(
              new PublishTask(
                  i, partSize, key, new IndexInputStream(input, filePosition, partSize), uploadId));
          filePosition += partSize;
        }
      }
    }

    synchronized void waitUntilDone() {
      while (true) {
        if (!done) {
          try {
            wait();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        } else {
          break;
        }
      }
      if (error != null) {
        throw error;
      }
    }

    synchronized void taskDone(int chunk, PartETag tag) {
      if (chunk == 0) {
        done = true;
        this.notifyAll();
      } else {
        tagList.add(tag);
        if (tagList.size() == chunkSize) {
          try {
            // Complete the multipart upload.
            CompleteMultipartUploadRequest compRequest =
                new CompleteMultipartUploadRequest(getBaseBucket(), key, uploadId, tagList);
            getS3Client().completeMultipartUpload(compRequest);
          } catch (Exception e) {
            error = new RuntimeException(e);
          }
          done = true;
          this.notifyAll();
        }
      }
    }

    synchronized void taskError(RuntimeException e) {
      done = true;
      error = e;
      this.notifyAll();
    }

    // Very rough. Bounds number of parts to at most 100, until file size > 500GB.
    long computeChunkSize(long fileLength) {
      if (fileLength <= FILE_5MB) {
        return fileLength;
      } else if (fileLength <= FILE_500MB) {
        return FILE_5MB;
      } else if (fileLength <= FILE_5GB) {
        return FILE_50MB;
      } else if (fileLength <= FILE_50GB) {
        return FILE_500MB;
      } else {
        return FILE_5GB;
      }
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    class PublishTask implements Runnable {
      private final int chunk;
      private final long chunkSize;
      private final String key;
      private final InputStream inputStream;
      private final String uploadId;

      public PublishTask(
          int chunk, long chunkSize, String key, InputStream inputStream, String uploadId) {
        this.chunk = chunk;
        this.chunkSize = chunkSize;
        this.key = key;
        this.inputStream = inputStream;
        this.uploadId = uploadId;
      }

      @Override
      public void run() {
        S3Thread s3Thread = (S3Thread) Thread.currentThread();
        while (true) {
          try {
            PartETag tag = null;
            if (chunk == 0) {
              ObjectMetadata metadata = new ObjectMetadata();
              metadata.setContentLength(chunkSize);
              s3Thread.getS3Client().putObject(getBaseBucket(), key, inputStream, metadata);
            } else {
              // Create the request to upload a part.
              UploadPartRequest uploadRequest =
                  new UploadPartRequest()
                      .withBucketName(getBaseBucket())
                      .withKey(key)
                      .withUploadId(uploadId)
                      .withPartNumber(chunk)
                      .withPartSize(chunkSize)
                      .withInputStream(inputStream);

              // Upload the part and add the response's ETag to our list.
              UploadPartResult uploadResult = getS3Client().uploadPart(uploadRequest);
              tag = uploadResult.getPartETag();
            }
            taskDone(chunk, tag);
            break;
          } catch (Exception e) {
            System.out.println("Got exception publishing chunk");
            e.printStackTrace();
          }
          try {
            int sleepTime = 1000;
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            taskError(new RuntimeException(e));
          }
        }
      }
    }
  }
}
