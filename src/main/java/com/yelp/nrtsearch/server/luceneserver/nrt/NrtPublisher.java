package com.yelp.nrtsearch.server.luceneserver.nrt;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

public class NrtPublisher {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int SEND_BUFFER_SIZE = 5 * 1024 * 1024;
  private static final String BASE_BUCKET = "nrtsearch-data";
  private static final String CLUSTER_NAME = "test-cluster";
  private final AmazonS3 s3Client;

  private Map<String, FileMetaData> lastPublishedFiles;
  private String lastPublishedStateFile = "";
  private final Directory directory;
  private final String dataBasePath;
  private final String stateBasePath;
  //private final String activeStatePath;

  private final byte[] sendBuffer = new byte[SEND_BUFFER_SIZE];
  private final InputStream sendStream = new ByteArrayInputStream(sendBuffer);

  private final Map<String, FileMetaData> pendingMergeFiles = new HashMap<>();


  public NrtPublisher(String indexName, int shardOrd, Directory directory, NrtActiveState activeState) throws IOException {
    this.directory = directory;

    s3Client = AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(new EndpointConfiguration("http://0.0.0.0:4566",Regions.DEFAULT_REGION.getName()))
        .withCredentials(new AWSStaticCredentialsProvider(new AWSCredentials() {
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

    dataBasePath = CLUSTER_NAME + "/" + indexName + "/" + shardOrd + "/data/";
    stateBasePath = CLUSTER_NAME + "/" + indexName + "/" + shardOrd + "/state/";
    //activeStatePath = CLUSTER_NAME + "/" + indexName + "/" + shardOrd + "/active_state";

    syncActiveNrtPoint(activeState);
  }

  public synchronized Map<String, FileMetaData> getLastPublishedFiles() {
    return lastPublishedFiles;
  }

  public synchronized void publishNrtPoint(CopyState copyState) throws IOException {
      String stateFile = getStateFileName(copyState);
      if (stateFile.equals(lastPublishedStateFile)) {
        System.out.println("Already published state, skipping: " + stateFile);
      }

      NrtPointState pointState = new NrtPointState(copyState);
      String stateStr = MAPPER.writeValueAsString(pointState);
      System.out.println("State: " + stateStr);

      // upload state
      s3Client.putObject(BASE_BUCKET, stateBasePath + stateFile, stateStr);

      List<String> filesToPublish = getFilesToPublish(copyState);
      System.out.println("Files to upload: " + filesToPublish);
      // upload files
      for (String file : filesToPublish) {
        publishFile(file);
      }

      // commit new point
      //NrtActiveState activeState = new NrtActiveState(stateFile);
      //setActive(activeState);

      lastPublishedFiles = copyState.files;
      lastPublishedStateFile = stateFile;

      synchronized (pendingMergeFiles) {
        for (String file : filesToPublish) {
          pendingMergeFiles.remove(file);
        }
      }
  }

  public void publishMergeFiles(Map<String, FileMetaData> files) throws IOException {
    for (Map.Entry<String, FileMetaData> entry : files.entrySet()) {
      System.out.println("Publishing merge file: " + entry.getKey());
      publishFile(entry.getKey());
    }
    synchronized (pendingMergeFiles) {
      pendingMergeFiles.putAll(files);
    }
  }

  public static String getStateFileName(CopyState copyState) {
    return String
        .format("%d_%d_%d_segments", copyState.primaryGen, copyState.gen, copyState.version);
  }

  public static class StateInfo {
    public long primaryGen;
    public long gen;
    public long version;
  }

  public static StateInfo getStateInfoFromFileName(String fileName) {
    String[] splits = fileName.split("_");
    StateInfo info = new StateInfo();
    info.primaryGen = Long.parseLong(splits[0]);
    info.gen = Long.parseLong(splits[1]);
    info.version = Long.parseLong(splits[2]);
    return info;
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
    String fileKey = dataBasePath + file;
    try (IndexInput indexInput = directory.openInput(file, IOContext.DEFAULT)) {
      long fileLength = indexInput.length();

      List<PartETag> partETags = new ArrayList<>();

      // Initiate the multipart upload.
      InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(BASE_BUCKET,
          fileKey);
      InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

      long filePosition = 0;
      for (int i = 1; filePosition < fileLength; i++) {
        // Because the last part could be less than 5 MB, adjust the part size as needed.
        long partSize = Math.min(SEND_BUFFER_SIZE, (fileLength - filePosition));

        indexInput.seek(filePosition);
        indexInput.readBytes(sendBuffer, 0, (int) partSize);

        sendStream.reset();

        // Create the request to upload a part.
        UploadPartRequest uploadRequest = new UploadPartRequest()
            .withBucketName(BASE_BUCKET)
            .withKey(fileKey)
            .withUploadId(initResponse.getUploadId())
            .withPartNumber(i)
            .withPartSize(partSize)
            .withInputStream(sendStream);

        // Upload the part and add the response's ETag to our list.
        UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
        partETags.add(uploadResult.getPartETag());

        filePosition += partSize;
      }

      // Complete the multipart upload.
      CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(BASE_BUCKET,
          fileKey,
          initResponse.getUploadId(), partETags);
      s3Client.completeMultipartUpload(compRequest);
    }
    System.out.println("Finished file upload: " + fileKey);
  }

  /*private void setActive(NrtActiveState activeState) throws IOException {
    String stateStr = MAPPER.writeValueAsString(activeState);
    s3Client.putObject(BASE_BUCKET, activeStatePath, stateStr);
  }*/

  private void syncActiveNrtPoint(NrtActiveState activeState) throws IOException {
    System.out.println("Sync active nrt point from s3");
    // we should be the only ones using the directory, but let's make sure
    try (Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
      Set<String> remainingFiles = new HashSet<>();
      CopyState activeCopyState = fetchActiveState(activeState);
      if (activeCopyState != null) {
        for (String file : directory.listAll()) {
          if (file.equals(IndexWriter.WRITE_LOCK_NAME)) {
            continue;
          }
          if (!activeCopyState.files.containsKey(file)) {
            System.out.println("Delete Extra file: " + file);
            directory.deleteFile(file);
          } else {
            remainingFiles.add(file);
          }
        }
        for (Map.Entry<String, FileMetaData> entry : activeCopyState.files.entrySet()) {
          if (remainingFiles.contains(entry.getKey()) && identicalFiles(entry.getKey(),
              entry.getValue())) {
            System.out.println("Local file identical to remote, skipping: " + entry.getKey());
            continue;
          }
          fetchFile(entry.getKey(), entry.getValue());
        }
        try (IndexOutput segmentOutput = directory.createOutput(
            IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", activeCopyState.gen),
            IOContext.DEFAULT)) {
          segmentOutput.writeBytes(activeCopyState.infosBytes, activeCopyState.infosBytes.length);
        }
        //directory.syncMetaData();
        //directory.sync(Collections.singleton("segments"));
        System.out.println("Final files: " + Arrays.toString(directory.listAll()));
        lastPublishedFiles = activeCopyState.files;
        lastPublishedStateFile = getStateFileName(activeCopyState);
      } else {
        lastPublishedFiles = Collections.emptyMap();
        lastPublishedStateFile = "";
      }
    }
  }

  private boolean identicalFiles(String file, FileMetaData metaData) throws IOException {
    FileMetaData localMetaData = getFileMetaData(file);
    if (localMetaData == null) {
      System.out.println("Could not read local metadata for file: " + file);
      return false;
    }
    //System.out.println("verify file: " + file);
    //System.out.println("len: " + localMetaData.length + ", " + metaData.length);
    //System.out.println("checksum: " + localMetaData.checksum + ", " + metaData.checksum);
    return localMetaData.length == metaData.length && localMetaData.checksum == metaData.checksum &&
        Arrays.equals(localMetaData.header, metaData.header) &&
        Arrays.equals(localMetaData.footer, metaData.footer);
  }

  private FileMetaData getFileMetaData(String file) throws IOException {
    long checksum;
    long length;
    byte[] header;
    byte[] footer;
    try (IndexInput in = directory.openInput(file, IOContext.DEFAULT)) {
      //System.out.println("could open dir for file: " + file);
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

  public CopyState fetchActiveState(NrtActiveState activeState) throws IOException {
    /*if (!s3Client.doesObjectExist(BASE_BUCKET, activeStatePath)) {
      return null;
    }
    String activeStateStr = s3Client.getObjectAsString(BASE_BUCKET, activeStatePath);

    String activeStateStr = activeState.stateFile;
    NrtActiveState activeState = MAPPER.readValue(activeStateStr, NrtActiveState.class);*/
    if (activeState == null) {
      return null;
    }
    if (activeState.stateFile.isEmpty()) {
      throw new IllegalStateException("Active state references an empty state name");
    }
    String activeStateKey = stateBasePath + activeState.stateFile;
    System.out.println("Fetching active state file: " + activeStateKey);
    String stateStr = s3Client.getObjectAsString(BASE_BUCKET, activeStateKey);
    NrtPointState pointState = MAPPER.readValue(stateStr, NrtPointState.class);
    return pointState.getCopyState();
  }

  private void fetchFile(String file, FileMetaData fileMetaData) throws IOException {
    /*IndexOutput indexOutput = directory.createOutput(file, IOContext.DEFAULT);
    String fileKey = dataBasePath + file;
    S3Object object = s3Client.getObject(BASE_BUCKET, fileKey);
    long length = object.getObjectMetadata().getContentLength();
    System.out.println("Obj : " + fileKey + ", len: " + length);
    byte[] data = object.getObjectContent().readAllBytes();
    System.out.println("data size: " + data.length);
    indexOutput.writeBytes(data, (int) length);
    indexOutput.close();
    directory.sync(Collections.singleton(file));
    directory.syncMetaData();
    if (!identicalFiles(file, fileMetaData)) {
      throw new IllegalArgumentException("Downloaded file does not match expected meta data: " + fileKey);
    }*/
    FSDirectory fsDirectory = getFSDirectory();
    File destFile = new File(fsDirectory.getDirectory().toFile(), file);

    String fileKey = dataBasePath + file;
    //System.out.println("File key: " + fileKey + ", file: " + destFile);
    s3Client.getObject(new GetObjectRequest(BASE_BUCKET, fileKey), destFile);
    //directory.syncMetaData();
    //directory.sync(Collections.singleton(file));
    //fsDirectory.syncMetaData();
    //fsDirectory.sync(Collections.singleton(file));
    //System.out.println("dirfiles: " + Arrays.toString(directory.listAll()));
    //System.out.println("fsdirfiles: " + Arrays.toString(fsDirectory.listAll()));
    if (!identicalFiles(file, fileMetaData)) {
      throw new IllegalArgumentException("Downloaded file does not match expected meta data: " + fileKey);
    }
  }

  private FSDirectory getFSDirectory() {
    Directory dir = directory;
    while (dir instanceof FilterDirectory) {
      dir = ((FilterDirectory) dir).getDelegate();
    }
    if (!(dir instanceof FSDirectory)) {
      throw new IllegalStateException("Base directory is not an FSDirectory");
    }
    return (FSDirectory) dir;
  }

  public DataInput getFileDataInput(String file) {
    String fileKey = dataBasePath + file;
    S3Object s3Object = s3Client.getObject(BASE_BUCKET, fileKey);
    return new S3DataInput(s3Object);
  }
}
