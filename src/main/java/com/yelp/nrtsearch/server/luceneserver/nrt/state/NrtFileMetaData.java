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
package com.yelp.nrtsearch.server.luceneserver.nrt.state;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtFileMetaData.NrtFileMetaDataDeserializer;
import java.io.IOException;
import org.apache.lucene.replicator.nrt.FileMetaData;

@JsonDeserialize(using = NrtFileMetaDataDeserializer.class)
public class NrtFileMetaData extends FileMetaData {

  public String pid;
  public long timestamp;

  public NrtFileMetaData(
      byte[] header, byte[] footer, long length, long checksum, String pid, long timestamp) {
    super(header, footer, length, checksum);
    this.pid = pid;
    this.timestamp = timestamp;
  }

  public NrtFileMetaData(FileMetaData metaData, String pid, long timestamp) {
    super(metaData.header, metaData.footer, metaData.length, metaData.checksum);
    this.pid = pid;
    this.timestamp = timestamp;
  }

  public static class NrtFileMetaDataDeserializer extends StdDeserializer<NrtFileMetaData> {

    public NrtFileMetaDataDeserializer() {
      this(null);
    }

    public NrtFileMetaDataDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public NrtFileMetaData deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      JsonNode node = jp.getCodec().readTree(jp);
      byte[] header = node.get("header").binaryValue();
      byte[] footer = node.get("footer").binaryValue();
      long length = node.get("length").longValue();
      long checksum = node.get("checksum").longValue();
      String pid = node.get("pid").textValue();
      long timestamp = node.get("timestamp").longValue();
      return new NrtFileMetaData(header, footer, length, checksum, pid, timestamp);
    }
  }
}
