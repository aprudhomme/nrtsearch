/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import java.io.IOException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NrtPointOnNewPrimaryTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void testNrtPointOnNewPrimary_disabled() throws IOException, InterruptedException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    primaryServer.verifySimpleDocs("test_index", 3);

    primaryServer.addSimpleDocs("test_index", 4, 5);
    primaryServer.refresh("test_index");

    primaryServer.verifySimpleDocs("test_index", 5);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withSyncInitialNrtPoint(false)
            .withAdditionalConfig(
                String.join(
                    "\n", "replicaReplicationPortPingInterval: 500", "nrtPointOnNewPrimary: false"))
            .build();
    replicaServer.verifySimpleDocs("test_index", 3);
    primaryServer.stopIndex("test_index");
    primaryServer.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("test_index").build());
    Thread.sleep(3000);
    replicaServer.verifySimpleDocs("test_index", 3);
  }

  @Test
  public void testNrtPointOnNewPrimary_enabled() throws IOException, InterruptedException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    primaryServer.verifySimpleDocs("test_index", 3);

    primaryServer.addSimpleDocs("test_index", 4, 5);
    primaryServer.refresh("test_index");

    primaryServer.verifySimpleDocs("test_index", 5);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withSyncInitialNrtPoint(false)
            .withAdditionalConfig(
                String.join(
                    "\n", "replicaReplicationPortPingInterval: 500", "nrtPointOnNewPrimary: true"))
            .build();
    replicaServer.verifySimpleDocs("test_index", 3);
    primaryServer.stopIndex("test_index");
    primaryServer.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("test_index").build());
    boolean passed = false;
    for (int i = 0; i < 60; ++i) {
      Thread.sleep(500);
      try {
        replicaServer.verifySimpleDocs("test_index", 5);
      } catch (AssertionError ignored) {
        continue;
      }
      passed = true;
      break;
    }
    assertTrue(passed);
  }
}
