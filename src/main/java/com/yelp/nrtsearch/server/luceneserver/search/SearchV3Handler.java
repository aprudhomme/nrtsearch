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
package com.yelp.nrtsearch.server.luceneserver.search;

import static com.yelp.nrtsearch.server.luceneserver.SearchHandler.getSearcherAndTaxonomy;

import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.concurrency.RequestExecutor;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchV3Handler {
  private static final Logger logger = LoggerFactory.getLogger(SearchV3Handler.class);

  private SearchV3Handler() {}

  public static void executeSearch(IndexState indexState, SearchRequest searchRequest, RequestExecutor<SearchResponse> requestExecutor) {
    long requestTimestamp = System.currentTimeMillis();
    var diagnostics = SearchResponse.Diagnostics.newBuilder();
    SearchContext searchContext;
    try {
      searchContext = buildSearchContext(searchRequest, indexState, diagnostics);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    // requestExecutor.addOnFinished(() -> SearchV3Handler.releaseContext(searchContext));


  }

  private static SearchContext buildSearchContext(
      SearchRequest searchRequest, IndexState indexState, Diagnostics.Builder diagnostics)
      throws IOException, InterruptedException {
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = getSearcherAndTaxonomy(searchRequest, indexState, shardState, diagnostics, null);
      return SearchRequestProcessor.buildContextForRequest(
          searchRequest, indexState, shardState, s, null);
    } catch (Throwable t) {
      try {
        if (s != null) {
          shardState.release(s);
        }
      } catch (IOException e) {
        logger.warn("Failed to release searcher reference previously acquired by acquire()", e);
        throw new RuntimeException(e);
      }
      throw t;
    }
  }

  private static void releaseContext(SearchContext context) {
    try {
      context.getShardState().release(context.getSearcherAndTaxonomy());
    } catch (IOException e) {
      logger.warn("Failed to release searcher reference previously acquired by acquire()", e);
      throw new RuntimeException(e);
    }
  }
}
