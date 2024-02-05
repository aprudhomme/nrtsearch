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
package com.yelp.nrtsearch.server.luceneserver.search;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to manage global ordinal lookup operations. Provides lookup to convert segment ordinals to
 * global ordinals, and to convert global ordinals to term strings.
 */
public abstract class GlobalOrdinalLookup {
  private static final Logger logger = LoggerFactory.getLogger(GlobalOrdinalLookup.class);
  static final LongValues IDENTITY_MAPPING = new IdentityMapping();
  static final Map<String, Map<BytesRef, String>> fieldStringCache = new ConcurrentHashMap<>();

  /**
   * Get segment mapping of local to global ordinals.
   *
   * @param segmentIndex index of segment in {@link IndexReader} leaf list
   */
  public abstract LongValues getSegmentMapping(int segmentIndex);

  /**
   * Look up term value for a given global ordinal.
   *
   * @param ord global ordinal
   * @throws IOException on error loading term value
   */
  public abstract String lookupGlobalOrdinal(long ord) throws IOException;

  /** Get the total number of global ordinals. */
  public abstract long getNumOrdinals();

  /** Implementation for field using {@link SortedDocValues}. */
  public static class SortedLookup extends GlobalOrdinalLookup {
    private final SortedDocValues sortedDocValues;
    private final String field;
    private final boolean preloadValues;
    private Int2ObjectMap<String> ordinalValueMap;

    public SortedLookup(IndexReader reader, String field, boolean preloadValues)
        throws IOException {
      this.field = field;
      this.preloadValues = preloadValues;
      sortedDocValues = MultiDocValues.getSortedValues(reader, field);
      if (preloadValues && sortedDocValues != null) {
        ordinalValueMap = new Int2ObjectOpenHashMap<>(sortedDocValues.getValueCount());
        GlobalOrdinalLookup.populateValueMap(field, ordinalValueMap, sortedDocValues);
      }
    }

    @Override
    public LongValues getSegmentMapping(int segmentIndex) {
      if (sortedDocValues == null || !(sortedDocValues instanceof MultiSortedDocValues)) {
        return IDENTITY_MAPPING;
      }
      return ((MultiSortedDocValues) sortedDocValues).mapping.getGlobalOrds(segmentIndex);
    }

    @Override
    public String lookupGlobalOrdinal(long ord) throws IOException {
      if (sortedDocValues == null) {
        throw new IllegalStateException("No ordinals for field: " + field);
      }
      if (!preloadValues) {
        return sortedDocValues.lookupOrd((int) ord).utf8ToString();
      } else {
        return ordinalValueMap.get((int) ord);
      }
    }

    @Override
    public long getNumOrdinals() {
      return sortedDocValues == null ? 0 : sortedDocValues.getValueCount();
    }
  }

  /** Implementation for field using {@link SortedSetDocValues}. */
  public static class SortedSetLookup extends GlobalOrdinalLookup {
    private final SortedSetDocValues sortedSetDocValues;
    private final String field;
    private boolean preloadValues;
    private Long2ObjectMap<String> ordinalValueMap;

    public SortedSetLookup(IndexReader reader, String field, boolean preloadValues)
        throws IOException {
      this.field = field;
      this.preloadValues = preloadValues;
      sortedSetDocValues = MultiDocValues.getSortedSetValues(reader, field);
      if (preloadValues && sortedSetDocValues != null) {
        ordinalValueMap = new Long2ObjectOpenHashMap<>((int) sortedSetDocValues.getValueCount());
        GlobalOrdinalLookup.populateValueMap(field, ordinalValueMap, sortedSetDocValues);
      }
    }

    @Override
    public LongValues getSegmentMapping(int segmentIndex) {
      if (sortedSetDocValues == null || !(sortedSetDocValues instanceof MultiSortedSetDocValues)) {
        return IDENTITY_MAPPING;
      }
      return ((MultiSortedSetDocValues) sortedSetDocValues).mapping.getGlobalOrds(segmentIndex);
    }

    @Override
    public String lookupGlobalOrdinal(long ord) throws IOException {
      if (sortedSetDocValues == null) {
        throw new IllegalStateException("No ordinals for field: " + field);
      }
      if (!preloadValues) {
        return sortedSetDocValues.lookupOrd(ord).utf8ToString();
      } else {
        return ordinalValueMap.get(ord);
      }
    }

    @Override
    public long getNumOrdinals() {
      return sortedSetDocValues == null ? 0 : sortedSetDocValues.getValueCount();
    }
  }

  private static void populateValueMap(
      String field, Int2ObjectMap<String> valueMap, SortedDocValues docValues) throws IOException {
    Map<BytesRef, String> stringCache = fieldStringCache.get(field);
    if (stringCache == null) {
      stringCache = new HashMap<>();
      fieldStringCache.put(field, stringCache);
    }

    for (int i = 0; i < docValues.getValueCount(); ++i) {
      BytesRef valueBytes = docValues.lookupOrd(i);
      String cachedValue = stringCache.get(valueBytes);
      if (cachedValue == null) {
        cachedValue = valueBytes.utf8ToString();
        stringCache.put(BytesRef.deepCopyOf(valueBytes), cachedValue);
      }
      valueMap.put(i, cachedValue);
    }
    logger.info("Cached string values, field: " + field + ", count: " + stringCache.size());
  }

  private static void populateValueMap(
      String field, Long2ObjectMap<String> valueMap, SortedSetDocValues docValues)
      throws IOException {
    Map<BytesRef, String> stringCache = fieldStringCache.get(field);
    if (stringCache == null) {
      stringCache = new HashMap<>();
      fieldStringCache.put(field, stringCache);
    }

    for (long i = 0; i < docValues.getValueCount(); ++i) {
      BytesRef valueBytes = docValues.lookupOrd(i);
      String cachedValue = stringCache.get(valueBytes);
      if (cachedValue == null) {
        cachedValue = valueBytes.utf8ToString();
        stringCache.put(BytesRef.deepCopyOf(valueBytes), cachedValue);
      }
      valueMap.put(i, cachedValue);
    }
    logger.info("Cached string values, field: " + field + ", count: " + stringCache.size());
  }

  /** Mapping that maps an ordinal to itself. */
  private static class IdentityMapping extends LongValues {

    @Override
    public long get(long index) {
      return index;
    }
  }
}
