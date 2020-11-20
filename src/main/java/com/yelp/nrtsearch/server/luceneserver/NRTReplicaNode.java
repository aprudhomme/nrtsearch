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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.FileMetadata;
import com.yelp.nrtsearch.server.grpc.FilesMetadata;
import com.yelp.nrtsearch.server.grpc.GetNodesResponse;
import com.yelp.nrtsearch.server.grpc.NodeInfo;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.luceneserver.nrt.NrtActiveState;
import com.yelp.nrtsearch.server.luceneserver.nrt.NrtFileMetaData;
import com.yelp.nrtsearch.server.luceneserver.nrt.NrtPublisher;
import com.yelp.nrtsearch.server.luceneserver.nrt.ReplicaCopyJob;
import com.yelp.nrtsearch.server.luceneserver.nrt.ReplicaStateManager;
import com.yelp.nrtsearch.server.luceneserver.nrt.ReplicaStateManager.InitialMergeState;
import com.yelp.nrtsearch.server.utils.HostPort;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.NodeCommunicationException;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NRTReplicaNode extends ReplicaNode {
  private final ReplicationServerClient primaryAddress;
  private final String indexName;
  final Jobs jobs;

  /* Just a wrapper class to hold our <hostName, port> pair so that we can send them to the Primary
   * on sendReplicas and it can build its channel over this pair */
  private final HostPort hostPort;
  Logger logger = LoggerFactory.getLogger(NRTPrimaryNode.class);

  private final NrtPublisher publisher;
  private final ReplicaStateManager replicaStateManager;

  private InitialMergesThread initialMergesThread;

  private ThreadPoolExecutor mergeThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

  public NRTReplicaNode(
      String indexName,
      ReplicationServerClient primaryAddress,
      HostPort hostPort,
      int replicaId,
      Directory indexDir,
      NrtPublisher publisher,
      ReplicaStateManager replicaStateManager,
      SearcherFactory searcherFactory,
      PrintStream printStream,
      long primaryGen)
      throws IOException {
    super(replicaId, indexDir, searcherFactory, printStream);
    this.primaryAddress = primaryAddress;
    this.indexName = indexName;
    this.hostPort = hostPort;
    this.publisher = publisher;
    this.replicaStateManager = replicaStateManager;
    // Handles fetching files from primary, on a new thread which receives files from primary
    jobs = new Jobs(this);
    jobs.setName("R" + id + ".copyJobs");
    jobs.setDaemon(true);
    jobs.start();
    lastFileMetaData = publisher.getLastPublishedFiles();
    start(primaryGen);
    replicaStateManager.getActiveStateSync();
    NrtActiveState currentActiveState = replicaStateManager.getCurrentActiveState();
    // force a blocking update to the current version
    if (currentActiveState != null) {
      CopyState copyState = publisher.fetchActiveState(currentActiveState);
      newNRTPoint(copyState.primaryGen, copyState.version);
    }
    // start async watcher for active state changes
    replicaStateManager.startStateUpdates();
  }

  @Override
  protected CopyJob newCopyJob(
      String reason,
      Map<String, FileMetaData> files,
      Map<String, FileMetaData> prevFiles,
      boolean highPriority,
      CopyJob.OnceDone onceDone)
      throws IOException {
    CopyState copyState;

    System.out.println("Create new copy job: " + reason);
    System.out.println("files: " + files);
    System.out.println("prevFiles: " + prevFiles);
    System.out.println("high priority: " + highPriority);

    // sendMeFiles(?) (we dont need this, just send Index,replica, and request for copy State)
    if (files == null) {
      // No incoming CopyState: ask primary for latest one now
      try {
        // Exceptions in here mean something went wrong talking over the socket, which are fine
        // (e.g. primary node crashed):
        //copyState = getCopyStateFromPrimary();
        copyState = publisher.fetchActiveState(replicaStateManager.getCurrentActiveState());
      } catch (Throwable t) {
        throw new NodeCommunicationException("exc while reading files to copy", t);
      }
      files = copyState.files;
    } else {
      copyState = null;
    }
    return new ReplicaCopyJob(
        reason, primaryAddress, copyState, this, files, highPriority, onceDone, indexName, publisher);
  }

  private CopyState getCopyStateFromPrimary() throws IOException {
    com.yelp.nrtsearch.server.grpc.CopyState copyState =
        primaryAddress.recvCopyState(indexName, id);
    return readCopyState(copyState);
  }

  /** Pulls CopyState off the wire */
  private static CopyState readCopyState(com.yelp.nrtsearch.server.grpc.CopyState copyState)
      throws IOException {

    // Decode a new CopyState
    byte[] infosBytes = new byte[copyState.getInfoBytesLength()];
    copyState.getInfoBytes().copyTo(ByteBuffer.wrap(infosBytes));

    long gen = copyState.getGen();
    long version = copyState.getVersion();
    Map<String, FileMetaData> files = readFilesMetaData(copyState.getFilesMetadata());

    int count = copyState.getCompletedMergeFilesCount();
    assert count == copyState.getCompletedMergeFilesCount();

    Set<String> completedMergeFiles = new HashSet<>();
    for (String completedMergeFile : copyState.getCompletedMergeFilesList()) {
      completedMergeFiles.add(completedMergeFile);
    }
    long primaryGen = copyState.getPrimaryGen();

    return new CopyState(files, version, gen, infosBytes, completedMergeFiles, primaryGen, null);
  }

  public static Map<String, FileMetaData> readFilesMetaData(FilesMetadata filesMetadata)
      throws IOException {
    int fileCount = filesMetadata.getNumFiles();
    assert fileCount == filesMetadata.getFileMetadataCount();

    Map<String, FileMetaData> files = new HashMap<>();
    for (FileMetadata fileMetadata : filesMetadata.getFileMetadataList()) {
      String fileName = fileMetadata.getFileName();
      long length = fileMetadata.getLen();
      long checksum = fileMetadata.getChecksum();
      byte[] header = new byte[fileMetadata.getHeaderLength()];
      fileMetadata.getHeader().copyTo(ByteBuffer.wrap(header));
      byte[] footer = new byte[fileMetadata.getFooterLength()];
      fileMetadata.getFooter().copyTo(ByteBuffer.wrap(footer));
      files.put(fileName, new FileMetaData(header, footer, length, checksum));
    }
    return files;
  }

  @Override
  protected void launch(CopyJob job) {
    jobs.launch(job);
  }

  /* called once start(primaryGen) is invoked on this object (see constructor) */
  @Override
  protected void sendNewReplica() throws IOException {
    logger.info(
        String.format(
            "send new_replica to primary host=%s, tcpPort=%s",
            primaryAddress.getHost(), primaryAddress.getPort()));
    // register self to process new merge warming
    replicaStateManager.startMergeHandling(this);
    System.out.println("Blocking for initial replica state");
    InitialMergeState initialState = replicaStateManager.blockForInitialMergeState();
    if (initialState != null) {
      System.out.println("Got initial merge state need to start warming: " + initialState.replicaMergeState.initialPendingMerges);
      startMergeTask(initialState.replicaMergeState.initialPendingMerges, initialState.primaryGen);
    } else {
      System.out.println("Could not get initial state, there may not be an active primary");
    }
    //primaryAddress.addReplicas(indexName, this.id, hostPort.getHostName(), hostPort.getPort());
  }

  public void startMergeTask(Map<String, NrtFileMetaData> files, long primaryGen) {
    mergeThreadPool.execute(new InitialMergesThread(files, primaryGen));
  }

  public CopyJob launchPreCopyFiles(
      AtomicBoolean finished, long curPrimaryGen, Map<String, FileMetaData> files)
      throws IOException {
    System.out.println("Start merge pre copy: " + files.keySet());
    return launchPreCopyMerge(finished, curPrimaryGen, files);
  }

  @Override
  public void close() throws IOException {
    jobs.close();
    logger.info("CLOSE NRT REPLICA");
    message("top: jobs closed");
    synchronized (mergeCopyJobs) {
      for (CopyJob job : mergeCopyJobs) {
        message("top: cancel merge copy job " + job);
        job.cancel("jobs closing", null);
      }
    }
    primaryAddress.close();
    super.close();
  }

  public ReplicationServerClient getPrimaryAddress() {
    return primaryAddress;
  }

  public HostPort getHostPort() {
    return hostPort;
  }

  /* returns true if present in primary's current list of known replicas else false.
  Throws StatusRuntimeException if cannot reach Primary */
  public boolean isKnownToPrimary() {
    GetNodesResponse getNodesResponse = primaryAddress.getConnectedNodes(indexName);
    for (NodeInfo nodeInfo : getNodesResponse.getNodesList()) {
      if (hostPort.equals(new HostPort(nodeInfo.getHostname(), nodeInfo.getPort()))) {
        return true;
      }
    }
    return false;
  }

  private class InitialMergesThread implements Runnable {
    private final Map<String, FileMetaData> files;
    private final long primaryGen;

    InitialMergesThread(Map<String, NrtFileMetaData> files, long primaryGen) {
      super();
      this.files = new HashMap<>();
      files.forEach((k, v) -> this.files.put(k, v.toFileMetaData()));
      this.primaryGen = primaryGen;
    }

    @Override
    public void run() {
      AtomicBoolean finished = new AtomicBoolean();
      try {
        CopyJob job = launchPreCopyFiles(finished, primaryGen, files);
      } catch (IOException e) {
        System.out.println(String.format("replica failed to launchPreCopyFiles" + files.keySet()));
        // called must set; //responseObserver.onError(e);
        throw new RuntimeException(e);
      }

      while (true) {
        // nocommit don't poll!  use a condition...
        if (finished.get()) {
          replicaStateManager.addWarmedMerges(files.keySet());
          System.out.println("replica is done copying files.." + files.keySet());
          break;
        }
        try {
          Thread.sleep(10);
          System.out.println("replica is copying files..." + files.keySet());
        } catch (InterruptedException e) {
          System.out.println(String.format("replica failed to copy files..." + files.keySet()));
          throw new RuntimeException(e);
        }
      }
    }
  }
}
