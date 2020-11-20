package com.yelp.nrtsearch.server.luceneserver.nrt;

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
