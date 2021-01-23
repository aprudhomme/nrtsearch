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
package com.yelp.nrtsearch.server.luceneserver.nrt.s3;

import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.store.IndexInput;

// TODO: implement more InputStream functionality. Maybe skip buffering if done in s3 client?
public class IndexInputStream extends InputStream {

  private final IndexInput inputCopy;
  private final long length;
  private long offset = 0;

  public IndexInputStream(IndexInput input, long start, long length) throws IOException {
    inputCopy = input.clone();
    inputCopy.seek(start);
    this.length = length;
  }

  @Override
  public int read() throws IOException {
    if (offset >= length) {
      return -1;
    }
    offset++;
    return Byte.toUnsignedInt(inputCopy.readByte());
  }
}
