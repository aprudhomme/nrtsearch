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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.remote.LZ4FileCompressor;
import com.yelp.nrtsearch.server.remote.ZstdFileCompressor;
import com.yelp.nrtsearch.tools.nrt_utils.backup.CompressionCommandUtils.FileStats;
import com.yelp.nrtsearch.tools.nrt_utils.backup.CompressionCommandUtils.TypeStats;
import java.util.List;
import org.junit.Test;

public class CompressionCommandUtilsTest {

  @Test
  public void testGetFileType() {
    assertEquals("fdt", CompressionCommandUtils.getFileType("_0.fdt"));
    assertEquals("dvd", CompressionCommandUtils.getFileType("_3x_Lucene90_0.dvd"));
    assertEquals("vec", CompressionCommandUtils.getFileType("_a_Lucene99HnswVectorsFormat_0.vec"));
    assertEquals("cfs", CompressionCommandUtils.getFileType("_1.cfs"));
    assertEquals("segments", CompressionCommandUtils.getFileType("segments_5"));
    assertEquals("other", CompressionCommandUtils.getFileType("other"));
  }

  @Test
  public void testSummarizeByFileType() {
    List<TypeStats> stats =
        CompressionCommandUtils.summarizeByFileType(
            List.of(
                new FileStats("_0.fdt", 100, 50, 10),
                new FileStats("_1_Lucene90_0.dvd", 300, 100, 20),
                new FileStats("_1.fdt", 200, 60, 5),
                new FileStats("segments_2", 10, 10, 0)));
    assertEquals(
        List.of(
            new TypeStats("dvd", 1, 300, 100, 20),
            new TypeStats("fdt", 2, 300, 110, 15),
            new TypeStats("segments", 1, 10, 10, 0)),
        stats);
  }

  @Test
  public void testSummarizeByFileType_empty() {
    assertEquals(List.of(), CompressionCommandUtils.summarizeByFileType(List.of()));
  }

  @Test
  public void testGetDecompressor() {
    assertTrue(CompressionCommandUtils.getDecompressor("LZ4") instanceof LZ4FileCompressor);
    assertTrue(CompressionCommandUtils.getDecompressor("ZSTD") instanceof ZstdFileCompressor);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> CompressionCommandUtils.getDecompressor("SNAPPY"));
    assertEquals("Unsupported compression type: SNAPPY", e.getMessage());
  }

  @Test
  public void testRates() {
    assertEquals(2.0, CompressionCommandUtils.ratio(200, 100), 0.0);
    assertEquals(0.0, CompressionCommandUtils.ratio(200, 0), 0.0);
    assertEquals(75.0, CompressionCommandUtils.spaceSavedPercent(400, 100), 0.0);
    assertEquals(0.0, CompressionCommandUtils.spaceSavedPercent(0, 0), 0.0);
    assertEquals(2.0, CompressionCommandUtils.mbPerSecond(2 * 1024 * 1024, 1_000_000_000L), 1e-9);
    assertEquals(0.0, CompressionCommandUtils.mbPerSecond(100, 0), 0.0);
  }
}
