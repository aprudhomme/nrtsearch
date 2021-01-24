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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.luceneserver.nrt.DataFileNameUtils;
import com.yelp.nrtsearch.server.luceneserver.nrt.StateFileNameUtils;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.ActiveState;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

public class S3DataManager {
  protected static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String BASE_BUCKET = "nrtsearch-data";
  private final String serviceName;
  private final AmazonS3 s3Client;

  private final Directory directory;

  private final String dataBasePath;
  private final String stateBasePath;

  public S3DataManager(String indexName, int shardOrd, String serviceName, Directory directory)
      throws IOException {
    this.serviceName = serviceName;
    this.directory = directory;

    // set up for localstack https://github.com/localstack/localstack
    s3Client =
        AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(
                new EndpointConfiguration("http://0.0.0.0:4566", Regions.DEFAULT_REGION.getName()))
            .withCredentials(
                new AWSStaticCredentialsProvider(
                    new AWSCredentials() {
                      @Override
                      public String getAWSAccessKeyId() {
                        return "test";
                      }

                      @Override
                      public String getAWSSecretKey() {
                        return "test";
                      }
                    }))
            .build();
    List<Bucket> buckets = s3Client.listBuckets();
    boolean found = false;
    for (Bucket bucket : buckets) {
      if (bucket.getName().equals(BASE_BUCKET)) {
        found = true;
        break;
      }
    }
    if (!found) {
      throw new IllegalStateException("Base bucket does not exist: " + BASE_BUCKET);
    }

    dataBasePath = serviceName + "/" + indexName + "/" + shardOrd + "/data/";
    stateBasePath = serviceName + "/" + indexName + "/" + shardOrd + "/state/";
  }

  protected AmazonS3 getS3Client() {
    return s3Client;
  }

  protected String getDataBasePath() {
    return dataBasePath;
  }

  protected String getStateBasePath() {
    return stateBasePath;
  }

  protected String getBaseBucket() {
    return BASE_BUCKET;
  }

  protected Directory getDirectory() {
    return directory;
  }

  public void close() throws IOException {
    s3Client.shutdown();
  }

  public void syncInitialNrtPoint(NrtPointState pointState) throws IOException {
    System.out.println("Sync nrt point from s3");
    // we should be the only ones using the directory, but let's make sure
    try (Lock writeLock = getDirectory().obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
      Set<String> remainingFiles = new HashSet<>();
      if (pointState != null) {
        for (String file : getDirectory().listAll()) {
          if (file.equals(IndexWriter.WRITE_LOCK_NAME)) {
            continue;
          }
          if (!pointState.files.containsKey(file)) {
            System.out.println("Delete Extra file: " + file);
            getDirectory().deleteFile(file);
          } else {
            remainingFiles.add(file);
          }
        }
        for (Map.Entry<String, NrtFileMetaData> entry : pointState.files.entrySet()) {
          if (remainingFiles.contains(entry.getKey())
              && identicalFiles(entry.getKey(), entry.getValue())) {
            System.out.println("Local file identical to remote, skipping: " + entry.getKey());
            continue;
          }
          fetchFile(entry.getKey(), entry.getValue());
        }
        try (IndexOutput segmentOutput =
            getDirectory()
                .createOutput(
                    IndexFileNames.fileNameFromGeneration(
                        IndexFileNames.SEGMENTS, "", pointState.gen),
                    IOContext.DEFAULT)) {
          segmentOutput.writeBytes(pointState.infosBytes, pointState.infosBytes.length);
        }
        System.out.println("Final files: " + Arrays.toString(getDirectory().listAll()));
      }
    }
  }

  private void fetchFile(String file, NrtFileMetaData fileMetaData) throws IOException {
    FSDirectory fsDirectory = getFSDirectory();
    File destFile = new File(fsDirectory.getDirectory().toFile(), file);

    String fileKey = getDataBasePath() + DataFileNameUtils.getDataFileName(fileMetaData, file);
    getS3Client().getObject(new GetObjectRequest(getBaseBucket(), fileKey), destFile);
    if (!identicalFiles(file, fileMetaData)) {
      throw new IllegalArgumentException(
          "Downloaded file does not match expected meta data: " + fileKey);
    }
  }

  private FSDirectory getFSDirectory() {
    Directory dir = getDirectory();
    while (dir instanceof FilterDirectory) {
      dir = ((FilterDirectory) dir).getDelegate();
    }
    if (!(dir instanceof FSDirectory)) {
      throw new IllegalStateException("Base directory is not an FSDirectory");
    }
    return (FSDirectory) dir;
  }

  public NrtPointState fetchActiveStateNrtPoint(ActiveState activeState) throws IOException {
    if (activeState == null) {
      return null;
    }
    String stateFile = StateFileNameUtils.getStateFileName(activeState);
    if (stateFile.isEmpty()) {
      throw new IllegalStateException("Active state references an empty state name");
    }
    String activeStateKey = stateBasePath + stateFile;
    System.out.println("Fetching active state file: " + activeStateKey);
    String stateStr = s3Client.getObjectAsString(BASE_BUCKET, activeStateKey);
    return MAPPER.readValue(stateStr, NrtPointState.class);
  }

  protected boolean identicalFiles(String file, FileMetaData metaData) throws IOException {
    FileMetaData localMetaData = getFileMetaData(file);
    if (localMetaData == null) {
      System.out.println("Could not read local metadata for file: " + file);
      return false;
    }
    return localMetaData.length == metaData.length
        && localMetaData.checksum == metaData.checksum
        && Arrays.equals(localMetaData.header, metaData.header)
        && Arrays.equals(localMetaData.footer, metaData.footer);
  }

  private FileMetaData getFileMetaData(String file) throws IOException {
    long checksum;
    long length;
    byte[] header;
    byte[] footer;
    try (IndexInput in = directory.openInput(file, IOContext.DEFAULT)) {
      // System.out.println("could open dir for file: " + file);
      try {
        length = in.length();
        header = CodecUtil.readIndexHeader(in);
        footer = CodecUtil.readFooter(in);
        checksum = CodecUtil.retrieveChecksum(in);
      } catch (EOFException | CorruptIndexException cie) {
        System.out.println("Exception: " + cie);
        cie.printStackTrace();
        return null;
      }
    } catch (FileNotFoundException | NoSuchFileException e) {
      System.out.println("Exception: " + e);
      e.printStackTrace();
      return null;
    }
    return new FileMetaData(header, footer, length, checksum);
  }
}
