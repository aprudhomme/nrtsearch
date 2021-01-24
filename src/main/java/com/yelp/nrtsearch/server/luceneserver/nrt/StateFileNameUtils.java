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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.replicator.nrt.CopyState;

public class StateFileNameUtils {
  private static final String STATE_FILE_FORMAT = "%d-[%s]-%d-%d-%d";
  private static final Pattern STATE_FILE_PATTERN =
      Pattern.compile("^(\\d+)-\\[(.+)]-(\\d+)-(\\d+)-(\\d+)$");

  private StateFileNameUtils() {}

  public static String getStateFileName(ActiveState activeState) {
    return activeState.stateFile;
  }

  public static String getStateFileName(NrtPointState pointState, String pid, long timestamp) {
    return String.format(
        STATE_FILE_FORMAT,
        timestamp,
        pid,
        pointState.primaryGen,
        pointState.gen,
        pointState.version);
  }

  public static String getStateFileName(CopyState copyState, String pid, long timestamp) {
    return String.format(
        STATE_FILE_FORMAT, timestamp, pid, copyState.primaryGen, copyState.gen, copyState.version);
  }

  public static class StateInfo {
    public long primaryGen;
    public long gen;
    public long version;
    public String pid;
    public long timestamp;
  }

  public static StateInfo getStateInfoFromFileName(String fileName) {
    Matcher matcher = STATE_FILE_PATTERN.matcher(fileName);
    if (matcher.matches()) {
      StateInfo info = new StateInfo();
      info.timestamp = Long.parseLong(matcher.group(1));
      info.pid = matcher.group(2);
      info.primaryGen = Long.parseLong(matcher.group(3));
      info.gen = Long.parseLong(matcher.group(4));
      info.version = Long.parseLong(matcher.group(5));
      return info;
    } else {
      throw new IllegalArgumentException("Unable to match against state file: " + fileName);
    }
  }
}
