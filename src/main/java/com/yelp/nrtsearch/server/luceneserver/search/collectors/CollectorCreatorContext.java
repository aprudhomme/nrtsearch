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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import com.yelp.nrtsearch.server.luceneserver.doc.DocLookup;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import java.util.Map;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;

/**
 * Context information needed to create collectors. Used for {@link DocCollector} and {@link
 * AdditionalCollectorManager} building.
 */
public class CollectorCreatorContext {
  private final int hitsToCollect;
  private final double searchTimeoutSec;
  private final int searchTimeoutCheckEvery;
  private final int searchTerminateAfter;
  private final boolean disallowPartialResults;
  private final boolean profile;
  private final DocLookup docLookup;
  private final Map<String, FieldDef> queryFields;
  private final SearcherAndTaxonomy searcherAndTaxonomy;

  /**
   * Constructor.
   *
   * @param hitsToCollect number of hits to collect
   * @param searchTimeoutSec timeout for recall phase, or 0 for none
   * @param searchTimeoutCheckEvery number of docs to collect in segment before checking timeout, or
   *     0 for segment boundary
   * @param searchTerminateAfter number of docs to collect before terminating collection early, or 0
   *     for unlimited
   * @param disallowPartialResults if a search timeout/terminate early should be an error condition
   * @param profile if extra profiling info should be collected
   * @param queryFields all possible fields usable for this query
   * @param searcherAndTaxonomy searcher for query
   */
  public CollectorCreatorContext(
      int hitsToCollect,
      double searchTimeoutSec,
      int searchTimeoutCheckEvery,
      int searchTerminateAfter,
      boolean disallowPartialResults,
      boolean profile,
      DocLookup docLookup,
      Map<String, FieldDef> queryFields,
      SearcherAndTaxonomy searcherAndTaxonomy) {
    this.hitsToCollect = hitsToCollect;
    this.searchTimeoutSec = searchTimeoutSec;
    this.searchTimeoutCheckEvery = searchTimeoutCheckEvery;
    this.searchTerminateAfter = searchTerminateAfter;
    this.disallowPartialResults = disallowPartialResults;
    this.profile = profile;
    this.docLookup = docLookup;
    this.queryFields = queryFields;
    this.searcherAndTaxonomy = searcherAndTaxonomy;
  }

  /** Get number of hits to collect during recall. */
  public int getHitsToCollect() {
    return hitsToCollect;
  }

  /** Get search timeout for recall phase, or 0 for none. */
  public double getSearchTimeoutSec() {
    return searchTimeoutSec;
  }

  /**
   * Get how may docs to collect in a segment before checking the search timeout, or 0 for segment
   * boundary only.
   */
  public int getSearchTimeoutCheckEvery() {
    return searchTimeoutCheckEvery;
  }

  /** Get how many documents to collect before terminating collection early, or 0 for unlimited. */
  public int getSearchTerminateAfter() {
    return searchTerminateAfter;
  }

  /**
   * Get if hitting the search timeout or terminate after limit is an error condition, otherwise
   * proceed with partial results.
   */
  public boolean getDisallowPartialResults() {
    return disallowPartialResults;
  }

  /** Get if search profiling info should be collected. */
  public boolean getProfile() {
    return profile;
  }

  /** Get doc value accessor for query fields. */
  public DocLookup getDocLookup() {
    return docLookup;
  }

  /** Get all possible field usable for this query */
  public Map<String, FieldDef> getQueryFields() {
    return queryFields;
  }

  /** Get searcher for query */
  public SearcherAndTaxonomy getSearcherAndTaxonomy() {
    return searcherAndTaxonomy;
  }
}
