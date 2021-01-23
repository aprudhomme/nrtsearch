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

import com.amazonaws.services.s3.AmazonS3;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

public class S3ThreadFactory implements ThreadFactory {

  private final Supplier<AmazonS3> s3Supplier;

  public S3ThreadFactory(Supplier<AmazonS3> s3Supplier) {
    this.s3Supplier = s3Supplier;
  }

  @Override
  public Thread newThread(Runnable r) {
    return new S3Thread(s3Supplier, r);
  }

  public static class S3Thread extends Thread {

    private final Supplier<AmazonS3> s3Supplier;

    public S3Thread(Supplier<AmazonS3> s3Supplier, Runnable r) {
      super(r);
      this.s3Supplier = s3Supplier;
    }

    public AmazonS3 getS3Client() {
      return s3Supplier.get();
    }
  }
}
