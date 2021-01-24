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

import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtFileMetaData;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataFileNameUtils {
  private static final String DATA_FILE_FORMAT = "%d-[%s]-%s";
  private static final Pattern DATA_FILE_PATTERN = Pattern.compile("^(\\d+)-\\[(.+)]-(.+)$");

  private DataFileNameUtils() {}

  public static String getDataFileName(String fileName, String pid, long timestamp) {
    return String.format(DATA_FILE_FORMAT, timestamp, pid, fileName);
  }

  public static String getDataFileName(NrtFileMetaData metaData, String fileName) {
    return String.format(DATA_FILE_FORMAT, metaData.timestamp, metaData.pid, fileName);
  }

  public static class DataInfo {
    public long timestamp;
    public String pid;
    public String fileName;
  }

  public static DataInfo getDataInfoFromFileName(String fileName) {
    Matcher matcher = DATA_FILE_PATTERN.matcher(fileName);
    if (matcher.matches()) {
      DataInfo info = new DataInfo();
      info.timestamp = Long.parseLong(matcher.group(1));
      info.pid = matcher.group(2);
      info.fileName = matcher.group(3);
      return info;
    } else {
      throw new IllegalArgumentException("Unable to match against state file: " + fileName);
    }
  }
}
