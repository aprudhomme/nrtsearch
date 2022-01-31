/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.search.cache;

/**
 * Interface for exposing metrics for a {@link org.apache.lucene.search.QueryCache} implementation.
 */
public interface CacheMetricsProvider {
  /** Get number of times {@link org.apache.lucene.search.DocIdSet} was found in cache. */
  long getHitCountMetric();
  /** Get number of times {@link org.apache.lucene.search.DocIdSet} was not found in cache. */
  long getMissCountMetric();
  /** Get number of {@link org.apache.lucene.search.DocIdSet} currently in the cache. */
  long getCacheSizeMetric();
  /** Get cache size in bytes. */
  long getRamBytesUsedMetric();
  /** Get number of {@link org.apache.lucene.search.DocIdSet} that have ever been cached. */
  long getCacheCountMetric();
  /** Get number of {@link org.apache.lucene.search.DocIdSet} that have been evicted. */
  long getEvictionCountMetric();
  /** Get count of all queries added to cache. */
  long getCacheQueryCountMetric();
  /** Get current number of queries in the cache. */
  long getCacheQuerySizeMetric();
}
