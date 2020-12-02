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

import com.amazonaws.services.s3.model.S3Object;
import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.store.DataInput;

public class S3DataInput extends DataInput implements Closeable {

  private final S3Object s3Object;

  public S3DataInput(S3Object s3Object) {
    System.out.println("Opening object data input: " + s3Object.getKey());
    this.s3Object = s3Object;
  }

  @Override
  public byte readByte() throws IOException {
    int b = s3Object.getObjectContent().read();
    if (b == -1) {
      throw new IOException("Eof reading from s3 object");
    }
    return (byte) b;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    int readBytes = 0;
    while (readBytes < len) {
      int readResult = s3Object.getObjectContent().read(b, offset + readBytes, len - readBytes);
      if (readResult == -1) {
        throw new IOException("Eof reading from s3 object");
      }
      readBytes += readResult;
    }
  }

  @Override
  public void close() throws IOException {
    System.out.println("Closing object input: " + s3Object.getKey());
    s3Object.close();
  }
}
