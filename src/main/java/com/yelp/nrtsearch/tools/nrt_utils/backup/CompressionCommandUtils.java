/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.tools.nrt_utils.backup;

import com.yelp.nrtsearch.server.remote.FileCompressor;
import com.yelp.nrtsearch.server.remote.LZ4FileCompressor;
import com.yelp.nrtsearch.server.remote.ZstdFileCompressor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Helpers shared by the snapshot compression testing commands. */
class CompressionCommandUtils {
  private static final double MIB = 1024 * 1024;

  private CompressionCommandUtils() {}

  /**
   * Statistics for a single index file.
   *
   * @param luceneFileName lucene index file name
   * @param uncompressedBytes uncompressed file size
   * @param compressedBytes compressed file size
   * @param nanos time spent compressing or decompressing the file
   */
  record FileStats(
      String luceneFileName, long uncompressedBytes, long compressedBytes, long nanos) {}

  /**
   * Statistics aggregated over all index files of one type.
   *
   * @param fileType file type, see {@link #getFileType(String)}
   * @param files number of files
   * @param uncompressedBytes total uncompressed size
   * @param compressedBytes total compressed size
   * @param nanos total time spent compressing or decompressing
   */
  record TypeStats(
      String fileType, int files, long uncompressedBytes, long compressedBytes, long nanos) {}

  /**
   * Get the type of lucene index file, which is the file extension (fdt, dvd, vec, etc.), or
   * "segments" for a segments_N file.
   *
   * @param luceneFileName lucene index file name
   * @return file type
   */
  static String getFileType(String luceneFileName) {
    int dot = luceneFileName.lastIndexOf('.');
    if (dot >= 0) {
      return luceneFileName.substring(dot + 1);
    }
    if (luceneFileName.startsWith("segments")) {
      return "segments";
    }
    return luceneFileName;
  }

  /**
   * Aggregate file statistics by file type, ordered by uncompressed size, largest first.
   *
   * @param files per file statistics
   * @return per file type statistics
   */
  static List<TypeStats> summarizeByFileType(List<FileStats> files) {
    Map<String, long[]> totals = new LinkedHashMap<>();
    for (FileStats file : files) {
      long[] total = totals.computeIfAbsent(getFileType(file.luceneFileName()), k -> new long[4]);
      total[0]++;
      total[1] += file.uncompressedBytes();
      total[2] += file.compressedBytes();
      total[3] += file.nanos();
    }
    List<TypeStats> stats = new ArrayList<>();
    totals.forEach(
        (type, total) ->
            stats.add(new TypeStats(type, (int) total[0], total[1], total[2], total[3])));
    stats.sort(
        Comparator.comparingLong(TypeStats::uncompressedBytes)
            .reversed()
            .thenComparing(TypeStats::fileType));
    return stats;
  }

  /**
   * Print a table of statistics aggregated by file type.
   *
   * @param files per file statistics
   * @param timeLabel name of the timed operation, such as "compress"
   */
  static void printFileTypeSummary(List<FileStats> files, String timeLabel) {
    System.out.println("  by file type:");
    System.out.println(
        String.format(
            "    %-10s %6s %16s %16s %7s %7s %12s %14s",
            "type",
            "files",
            "uncompressed",
            "compressed",
            "ratio",
            "saved",
            timeLabel + " s",
            "MB/s/thread"));
    for (TypeStats stats : summarizeByFileType(files)) {
      System.out.println(
          String.format(
              "    %-10s %6d %16d %16d %7.3f %6.1f%% %12.3f %14.1f",
              stats.fileType(),
              stats.files(),
              stats.uncompressedBytes(),
              stats.compressedBytes(),
              ratio(stats.uncompressedBytes(), stats.compressedBytes()),
              spaceSavedPercent(stats.uncompressedBytes(), stats.compressedBytes()),
              stats.nanos() / 1e9,
              mbPerSecond(stats.uncompressedBytes(), stats.nanos())));
    }
  }

  /**
   * Get a compressor that can decompress files of the given built-in compression type.
   *
   * @param compressionType compression type recorded in the file metadata
   * @return compressor
   * @throws IllegalArgumentException if the compression type is not a built-in type
   */
  static FileCompressor getDecompressor(String compressionType) {
    return switch (compressionType) {
      case "LZ4" -> new LZ4FileCompressor();
      case "ZSTD" ->
          new ZstdFileCompressor(
              ZstdFileCompressor.DEFAULT_LEVEL, ZstdFileCompressor.DEFAULT_WORKERS);
      default ->
          throw new IllegalArgumentException("Unsupported compression type: " + compressionType);
    };
  }

  static double ratio(long uncompressed, long compressed) {
    return compressed == 0 ? 0.0 : (double) uncompressed / compressed;
  }

  static double spaceSavedPercent(long uncompressed, long compressed) {
    return uncompressed == 0 ? 0.0 : 100.0 * (uncompressed - compressed) / uncompressed;
  }

  /** Throughput in MB/s, where 1 MB is 2^20 bytes. */
  static double mbPerSecond(long bytes, long nanos) {
    return nanos == 0 ? 0.0 : bytes / (nanos / 1e9) / MIB;
  }
}
