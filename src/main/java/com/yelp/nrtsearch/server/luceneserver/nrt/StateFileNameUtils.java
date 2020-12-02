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
package com.yelp.nrtsearch.server.luceneserver.nrt;

import com.yelp.nrtsearch.server.luceneserver.nrt.state.ActiveState;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import org.apache.lucene.replicator.nrt.CopyState;

public class StateFileNameUtils {
  private static final String STATE_FILE_FORMAT = "%d_%d_%d_segments";

  private StateFileNameUtils() {}

  public static String getStateFileName(ActiveState activeState) {
    return activeState.stateFile;
  }

  public static String getStateFileName(NrtPointState pointState) {
    return String.format(
        STATE_FILE_FORMAT, pointState.primaryGen, pointState.gen, pointState.version);
  }

  public static String getStateFileName(CopyState copyState) {
    return String.format(STATE_FILE_FORMAT, copyState.primaryGen, copyState.gen, copyState.version);
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
}
