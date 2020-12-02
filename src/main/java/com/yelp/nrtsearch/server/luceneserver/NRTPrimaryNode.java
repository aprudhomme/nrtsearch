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

import com.yelp.nrtsearch.server.grpc.FilesMetadata;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.luceneserver.nrt.PrimaryDataManager;
import com.yelp.nrtsearch.server.luceneserver.nrt.PrimaryStateManager;
import com.yelp.nrtsearch.server.utils.HostPort;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.PrimaryNode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NRTPrimaryNode extends PrimaryNode {
  private final HostPort hostPort;
  private final String indexName;
  Logger logger = LoggerFactory.getLogger(NRTPrimaryNode.class);
  final List<MergePreCopy> warmingSegments = Collections.synchronizedList(new ArrayList<>());
  final Queue<ReplicaDetails> replicasInfos = new ConcurrentLinkedQueue<>();

  private final PrimaryDataManager primaryDataManager;
  private final PrimaryStateManager primaryStateManager;

  public NRTPrimaryNode(
      String indexName,
      HostPort hostPort,
      IndexWriter writer,
      PrimaryDataManager primaryDataManager,
      PrimaryStateManager primaryStateManager,
      int id,
      long primaryGen,
      long forcePrimaryVersion,
      SearcherFactory searcherFactory,
      PrintStream printStream)
      throws IOException {
    super(writer, id, primaryGen, forcePrimaryVersion, searcherFactory, printStream);
    this.hostPort = hostPort;
    this.indexName = indexName;
    this.primaryDataManager = primaryDataManager;
    this.primaryStateManager = primaryStateManager;
  }

  public static class ReplicaDetails {
    private final int replicaId;
    private final HostPort hostPort;
    private final ReplicationServerClient replicationServerClient;

    public int getReplicaId() {
      return replicaId;
    }

    public ReplicationServerClient getReplicationServerClient() {
      return replicationServerClient;
    }

    public HostPort getHostPort() {
      return hostPort;
    }

    ReplicaDetails(int replicaId, ReplicationServerClient replicationServerClient) {
      this.replicaId = replicaId;
      this.replicationServerClient = replicationServerClient;
      this.hostPort =
          new HostPort(replicationServerClient.getHost(), replicationServerClient.getPort());
    }

    /*
     * WARNING: Do not replace this with the IDE autogenerated equals method. We put this object in a ConcurrentQueue
     * and this is the check that we need for equality.
     * */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReplicaDetails that = (ReplicaDetails) o;
      return replicaId == that.replicaId && Objects.equals(hostPort, that.hostPort);
    }

    /*
     * WARNING: Do not replace this with the IDE autogenerated hashCode method. We put this object in a ConcurrentQueue
     * and this is the check that we need for hashCode.
     * */
    @Override
    public int hashCode() {
      return Objects.hash(replicaId, hostPort);
    }
  }

  /** Holds all replicas currently warming (pre-copying the new files) a single merged segment */
  static class MergePreCopy {
    final List<ReplicationServerClient> connections =
        Collections.synchronizedList(new ArrayList<>());
    final Map<String, FileMetaData> files;
    private boolean finished;

    public MergePreCopy(Map<String, FileMetaData> files) {
      this.files = files;
    }

    public synchronized boolean tryAddConnection(ReplicationServerClient c) {
      if (finished == false) {
        connections.add(c);
        return true;
      } else {
        return false;
      }
    }

    public synchronized boolean finished() {
      if (connections.isEmpty()) {
        finished = true;
        return true;
      } else {
        return false;
      }
    }
  }

  public void publishNrtPoint() throws IOException {
    CopyState copyState = getCopyState();
    try {
      primaryDataManager.publishNrtPoint(copyState);
      primaryStateManager.updateActiveStateWithCopyState(copyState);
    } finally {
      releaseCopyState(copyState);
    }
  }

  // TODO: awkward we are forced to do this here ... this should really live in replicator code,
  // e.g. PrimaryNode.mgr should be this:
  static class PrimaryNodeReferenceManager extends ReferenceManager<IndexSearcher> {
    final NRTPrimaryNode primary;
    final SearcherFactory searcherFactory;

    public PrimaryNodeReferenceManager(NRTPrimaryNode primary, SearcherFactory searcherFactory)
        throws IOException {
      this.primary = primary;
      this.searcherFactory = searcherFactory;
      current =
          SearcherManager.getSearcher(
              searcherFactory, primary.mgr.acquire().getIndexReader(), null);
    }

    @Override
    protected void decRef(IndexSearcher reference) throws IOException {
      reference.getIndexReader().decRef();
    }

    @Override
    protected IndexSearcher refreshIfNeeded(IndexSearcher referenceToRefresh) throws IOException {
      if (primary.flushAndRefresh()) {
        primary.publishNrtPoint();
        // primary.sendNewNRTPointToReplicas();
        // NOTE: steals a ref from one ReferenceManager to another!
        return SearcherManager.getSearcher(
            searcherFactory,
            primary.mgr.acquire().getIndexReader(),
            referenceToRefresh.getIndexReader());
      } else {
        return null;
      }
    }

    @Override
    protected boolean tryIncRef(IndexSearcher reference) {
      return reference.getIndexReader().tryIncRef();
    }

    @Override
    protected int getRefCount(IndexSearcher reference) {
      return reference.getIndexReader().getRefCount();
    }
  }

  @Override
  protected void preCopyMergedSegmentFiles(SegmentCommitInfo info, Map<String, FileMetaData> files)
      throws IOException {

    System.out.println("start merge: " + info.info);
    primaryDataManager.publishMergeFiles(files);
    primaryStateManager.warmMergeFiles(files);
  }

  public void setRAMBufferSizeMB(double mb) {
    writer.getConfig().setRAMBufferSizeMB(mb);
  }

  public void addReplica(int replicaID, ReplicationServerClient replicationServerClient)
      throws IOException {
    logger.info("add replica: " + warmingSegments.size() + " current warming merges ");
    message("add replica: " + warmingSegments.size() + " current warming merges ");
    ReplicaDetails replicaDetails = new ReplicaDetails(replicaID, replicationServerClient);
    if (!replicasInfos.contains(replicaDetails)) {
      replicasInfos.add(replicaDetails);
    }
    // Step through all currently warming segments and try to add this replica if it isn't there
    // already:
    synchronized (warmingSegments) {
      for (MergePreCopy preCopy : warmingSegments) {
        logger.debug(String.format("warming segment %s", preCopy.files.keySet()));
        message("warming segment " + preCopy.files.keySet());
        boolean found = false;
        synchronized (preCopy.connections) {
          for (ReplicationServerClient each : preCopy.connections) {
            if (each.equals(replicationServerClient)) {
              found = true;
              break;
            }
          }
        }

        if (found) {
          logger.info(String.format("this replica is already warming this segment; skipping"));
          message("this replica is already warming this segment; skipping");
          // It's possible (maybe) that the replica started up, then a merge kicked off, and it
          // warmed to this new replica, all before the
          // replica sent us this command:
          continue;
        }

        // OK, this new replica is not already warming this segment, so attempt (could fail) to
        // start warming now:
        if (preCopy.tryAddConnection(replicationServerClient) == false) {
          // This can happen, if all other replicas just now finished warming this segment, and so
          // we were just a bit too late.  In this
          // case the segment must be copied over in the next nrt point sent to this replica
          logger.info("failed to add connection to segment warmer (too late); closing");
          message("failed to add connection to segment warmer (too late); closing");
          // TODO: Close this and other replicationServerClient in close of this class? c.close();
        }

        FilesMetadata filesMetadata = RecvCopyStateHandler.writeFilesMetaData(preCopy.files);
        replicationServerClient.copyFiles(indexName, primaryGen, filesMetadata);
        logger.info("successfully started warming");
        message("successfully started warming");
      }
    }
  }

  public Collection<ReplicaDetails> getNodesInfo() {
    return Collections.unmodifiableCollection(replicasInfos);
  }

  @Override
  public void close() throws IOException {
    logger.info("CLOSE NRT PRIMARY");
    Iterator<ReplicaDetails> it = replicasInfos.iterator();
    while (it.hasNext()) {
      ReplicaDetails replicaDetails = it.next();
      ReplicationServerClient replicationServerClient = replicaDetails.getReplicationServerClient();
      HostPort replicaHostPort = replicaDetails.getHostPort();
      logger.info(
          String.format(
              "CLOSE NRT PRIMARY, closing replica channel host:%s, port:%s",
              replicaHostPort.getHostName(), replicaHostPort.getPort()));
      replicationServerClient.close();
      it.remove();
    }
    super.close();
  }
}
