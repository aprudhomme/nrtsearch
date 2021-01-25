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

import com.yelp.nrtsearch.server.luceneserver.nrt.state.ActiveState;
import com.yelp.nrtsearch.server.luceneserver.nrt.zookeeper.ZkStateManager;
import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;

public class ZkCleanupManager extends ZkStateManager {

  public ZkCleanupManager(String indexName, int shardOrd, String serviceName) throws IOException {
    super(indexName, shardOrd, serviceName);
  }

  public ActiveState getActiveState() {
    String activeStateJson;
    try {
      while (true) {
        try {
          activeStateJson = new String(getZk().getData(getActiveStatePath(), false, null));
          break;
        } catch (ConnectionLossException ignored) {

        } catch (NoNodeException e) {
          throw new IllegalStateException("There is no active_state node", e);
        } catch (KeeperException e) {
          throw new RuntimeException("Error creating getting active_state: " + e);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted getting active state", e);
    }
    try {
      if (!activeStateJson.isEmpty()) {
        return MAPPER.readValue(activeStateJson, ActiveState.class);
      } else {
        return null;
      }
    } catch (IOException e) {
      throw new IllegalStateException("Error reading active state json", e);
    }
  }
}
