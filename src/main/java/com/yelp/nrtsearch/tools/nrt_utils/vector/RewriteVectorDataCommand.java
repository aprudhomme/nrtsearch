/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.tools.nrt_utils.vector;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene90.Lucene90CompoundFormat;
import org.apache.lucene.codecs.lucene90.Lucene90SegmentInfoFormat;
import org.apache.lucene.codecs.lucene94.Lucene94FieldInfosFormat;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.codecs.lucene95.MyHnswVectorsWriter;
import org.apache.lucene.codecs.lucene95.MyHnswVectorsWriter.RebuildStats;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.PrintStreamInfoStream;
import picocli.CommandLine;

@CommandLine.Command(
    name = RewriteVectorDataCommand.REWRITE_VECTOR_DATA,
    description = "Rewrite vector data with new indexing parameters.")
public class RewriteVectorDataCommand implements Callable<Integer> {
  public static final String REWRITE_VECTOR_DATA = "rewriteVectorData";

  private static final Lucene90SegmentInfoFormat siFormat = new Lucene90SegmentInfoFormat();
  private static final Lucene90CompoundFormat compoundFormat = new Lucene90CompoundFormat();
  private static final Lucene94FieldInfosFormat fieldInfosFormat = new Lucene94FieldInfosFormat();
  // The build params here are not actually used
  private static final Lucene95HnswVectorsFormat vectorsFormat =
      new Lucene95HnswVectorsFormat(1, 1);
  private static final Pattern idPattern = Pattern.compile("^.*got=([a-z0-9]+)\\s(.*)");

  @CommandLine.Option(
      names = {"-d", "--dataDir"},
      description = "Directory with source vector index files",
      required = true)
  private String dataPath;

  @CommandLine.Option(
      names = {"-o", "--outDir"},
      description = "Directory to write new vector index files",
      required = true)
  private String outPath;

  @CommandLine.Option(
      names = {"-m"},
      description = "Hnsw m param",
      defaultValue = "16")
  private int m;

  @CommandLine.Option(
      names = {"--beamWidth"},
      description = "Hnsw beam width param",
      defaultValue = "100")
  private int beamWidth;

  @CommandLine.Option(
      names = {"-p"},
      description = "Number of segments to rewrite in parallel",
      defaultValue = "4")
  private int parallelism;

  @Override
  public Integer call() throws Exception {
    ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(parallelism);

    try {
      List<Future<RebuildStats>> futures = new ArrayList<>();
      try (Directory dataDir = FSDirectory.open(Path.of(dataPath));
          Directory outDir = FSDirectory.open(Path.of(outPath))) {
        List<String> segmentNames = getAllSegments(dataDir);
        System.out.println("Segments: " + segmentNames);

        for (String segmentName : segmentNames) {
          futures.add(
              threadPoolExecutor.submit(
                  () -> {
                    try {
                      return rewriteSegment(segmentName, dataDir, outDir);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }));
        }
        List<RebuildStats> statsList = new ArrayList<>();
        for (Future<RebuildStats> future : futures) {
          statsList.add(future.get());
        }

        outputStats(statsList);
      }
    } finally {
      threadPoolExecutor.shutdownNow();
    }
    return 0;
  }

  RebuildStats rewriteSegment(String segmentName, Directory dataDir, Directory outDir)
      throws IOException {
    System.out.println("Process segment: " + segmentName);
    byte[] segmentId = getSegmentId(dataDir, segmentName);
    SegmentInfo si = siFormat.read(dataDir, segmentName, segmentId, IOContext.DEFAULT);

    Directory segmentDataDir;
    if (si.getUseCompoundFile()) {
      segmentDataDir = compoundFormat.getCompoundReader(dataDir, si, IOContext.DEFAULT);
    } else {
      segmentDataDir = dataDir;
    }
    String suffix = getVecSegmentSuffix(segmentDataDir);
    RebuildStats rebuildStats;
    if (suffix != null) {
      FieldInfos fieldInfos = fieldInfosFormat.read(segmentDataDir, si, "", IOContext.DEFAULT);
      SegmentReadState readState =
          new SegmentReadState(segmentDataDir, si, fieldInfos, IOContext.DEFAULT, suffix);
      try (KnnVectorsReader vectorReader = vectorsFormat.fieldsReader(readState)) {
        FloatVectorValues vectorValues =
            vectorReader.getFloatVectorValues("photo_embeddings_clip_v1_features");
        System.out.println("Size: " + vectorValues.size());

        SegmentWriteState writeState =
            new SegmentWriteState(
                new PrintStreamInfoStream(System.out),
                outDir,
                si,
                fieldInfos,
                null,
                IOContext.DEFAULT,
                suffix);
        try (MyHnswVectorsWriter vectorsWriter =
            new MyHnswVectorsWriter(writeState, m, beamWidth)) {
          rebuildStats =
              vectorsWriter.rebuildFromReader(
                  "photo_embeddings_clip_v1_features", fieldInfos, vectorReader);
          vectorsWriter.finish();
        }
      }
      if (si.getUseCompoundFile()) {
        System.out.println("Rebuild compound file");
        Directory rewriteDir = new RewriteCompoundDirectory(segmentDataDir, outDir);
        si.setFiles(List.of(segmentDataDir.listAll()));
        compoundFormat.write(rewriteDir, si, IOContext.DEFAULT);
        deleteVectorFiles(outDir, segmentName, suffix);
      }
    } else {
      System.out.println("Skipping segment with no vector data");
      rebuildStats = new RebuildStats();
    }
    return rebuildStats;
  }

  List<String> getAllSegments(Directory dir) throws IOException {
    List<String> segmentNames = new ArrayList<>();
    for (String file : dir.listAll()) {
      if (IndexFileNames.matchesExtension(file, "si")) {
        segmentNames.add(IndexFileNames.stripExtension(file));
      }
    }
    return segmentNames;
  }

  byte[] getSegmentId(Directory dir, String segmentName) throws IOException {
    try {
      siFormat.read(dir, segmentName, new byte[0], IOContext.DEFAULT);
    } catch (CorruptIndexException e) {
      Matcher idMatcher = idPattern.matcher(e.getMessage());
      if (idMatcher.find()) {
        String idStr = idMatcher.group(1);
        byte[] segmentId = new BigInteger(idStr, Character.MAX_RADIX).toByteArray();
        if (segmentId.length == 17) {
          byte[] truncatedId = new byte[16];
          System.arraycopy(segmentId, 1, truncatedId, 0, 16);
          return truncatedId;
        } else {
          return segmentId;
        }
      }
      throw new RuntimeException(e);
    }
    throw new RuntimeException("Did not get id exception?");
  }

  String getVecSegmentSuffix(Directory directory) throws IOException {
    for (String file : directory.listAll()) {
      if (IndexFileNames.matchesExtension(file, "vem")) {
        return IndexFileNames.stripSegmentName(IndexFileNames.stripExtension(file)).substring(1);
      }
    }
    return null;
  }

  void deleteVectorFiles(Directory directory, String segmentName, String suffix)
      throws IOException {
    directory.deleteFile(segmentName + "_" + suffix + ".vec");
    directory.deleteFile(segmentName + "_" + suffix + ".vex");
    directory.deleteFile(segmentName + "_" + suffix + ".vem");
  }

  void outputStats(List<RebuildStats> statsList) {
    long totalGraphBuildTimeNs = 0;
    long totalGraphSizeBytes = 0;
    long totalVecDataSizeBytes = 0;
    long totalVectorCount = 0;
    for (RebuildStats stats : statsList) {
      totalGraphBuildTimeNs += stats.buildTimeNs;
      totalGraphSizeBytes += stats.graphSizeBytes;
      totalVecDataSizeBytes += stats.vecDataSizeBytes;
      totalVectorCount += stats.vectorCount;
    }
    System.out.println(
        "Graph Build time: "
            + (totalGraphBuildTimeNs / 1000000000.0)
            + "s, size: "
            + totalGraphSizeBytes
            + ", vectors: "
            + totalVectorCount
            + ", size: "
            + totalVecDataSizeBytes);
  }
}
