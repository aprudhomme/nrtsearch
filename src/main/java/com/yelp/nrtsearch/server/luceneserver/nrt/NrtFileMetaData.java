package com.yelp.nrtsearch.server.luceneserver.nrt;

import org.apache.lucene.replicator.nrt.FileMetaData;

public class NrtFileMetaData {

  // Header and footer of the file must be identical between primary and replica to consider the files equal:
  public byte[] header;
  public byte[] footer;

  public long length;

  // Used to ensure no bit flips when copying the file:
  public long checksum;

  public NrtFileMetaData() {}

  public NrtFileMetaData(FileMetaData fileMetaData) {
    this.header = fileMetaData.header;
    this.footer = fileMetaData.footer;
    this.length = fileMetaData.length;
    this.checksum = fileMetaData.checksum;
  }

  public NrtFileMetaData(byte[] header, byte[] footer, long length, long checksum) {
    this.header = header;
    this.footer = footer;
    this.length = length;
    this.checksum = checksum;
  }

  public FileMetaData toFileMetaData() {
    return new FileMetaData(header, footer, length, checksum);
  }

  @Override
  public String toString() {
    return "FileMetaData(length=" + length + ")";
  }
}
