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

import com.yelp.nrtsearch.server.luceneserver.nrt.DataFileNameUtils;
import com.yelp.nrtsearch.server.luceneserver.nrt.StateFileNameUtils;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.ActiveState;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import picocli.CommandLine;

@CommandLine.Command(name = "nrt-cleanup", description = "Cleanup unneeded data/state files")
public class NrtCleanup implements Runnable {
  private static final long MIN_TO_MS = 60 * 1000;

  public static void main(String[] args) {
    System.exit(new CommandLine(new NrtCleanup()).execute(args));
  }

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Name of the index to be cleaned up",
      required = true)
  private String indexName;

  @CommandLine.Option(
      names = {"-c", "--clusterName"},
      description = "Name of the cluster to be cleaned up",
      required = true)
  private String clusterName;

  @CommandLine.Option(
      names = {"-s", "--shard"},
      description = "Shard number to be cleaned up, default: 0")
  private int shard = 0;

  @CommandLine.Option(
      names = {"-g", "--graceMinutes"},
      description = "Do not delete files younger than this many minutes, default: 60")
  private long graceMinutes = 60;

  @Override
  public void run() {
    System.out.println("Running cleanup command");

    ZkCleanupManager zkCleanupManager = null;
    S3CleanupManager s3CleanupManager = null;
    try {
      try {
        zkCleanupManager = new ZkCleanupManager(indexName, shard, clusterName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      ActiveState activeState = zkCleanupManager.getActiveState();
      if (activeState == null) {
        throw new IllegalStateException("Index has no active state");
      }

      try {
        s3CleanupManager = new S3CleanupManager(indexName, shard, clusterName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      Set<String> referencedFiles = new HashSet<>();
      Set<String> referencedStates = new HashSet<>();
      // add active state
      NrtPointState pointState;
      try {
        pointState = s3CleanupManager.fetchActiveStateNrtPoint(activeState);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      referencedStates.add(activeState.stateFile);
      for (Map.Entry<String, NrtFileMetaData> entry : pointState.files.entrySet()) {
        referencedFiles.add(DataFileNameUtils.getDataFileName(entry.getValue(), entry.getKey()));
      }

      // TODO: add pending merges
      // TODO: add snapshots

      System.out.println("Referenced files: " + referencedFiles);
      System.out.println("Referenced states: " + referencedStates);

      // verify referenced files exist
      s3CleanupManager.verifyDataFilesExists(referencedFiles);
      s3CleanupManager.verifyStateFilesExists(referencedStates);

      long maxTimestamp = Instant.now().toEpochMilli() - (graceMinutes * MIN_TO_MS);
      maxTimestamp =
          Math.min(
              maxTimestamp,
              StateFileNameUtils.getStateInfoFromFileName(activeState.stateFile).timestamp);

      // cleanup data files
      s3CleanupManager.cleanupDataFiles(referencedFiles, maxTimestamp);

      // cleanup state files
      s3CleanupManager.cleanupStateFiles(referencedStates, maxTimestamp);
    } finally {
      if (s3CleanupManager != null) {
        try {
          s3CleanupManager.close();
        } catch (Throwable ignore) {

        }
      }
      if (zkCleanupManager != null) {
        zkCleanupManager.close();
      }
    }
  }
}
