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

import static com.yelp.nrtsearch.server.luceneserver.SearchHandler.fetchFields;
import static com.yelp.nrtsearch.server.luceneserver.SearchHandler.findTimeoutException;
import static com.yelp.nrtsearch.server.luceneserver.SearchHandler.getHitsFromOffset;
import static com.yelp.nrtsearch.server.luceneserver.SearchHandler.getSearcherAndTaxonomy;
import static com.yelp.nrtsearch.server.luceneserver.SearchHandler.setResponseHits;

import com.yelp.nrtsearch.server.grpc.DeadlineUtils;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.MyIndexSearcher;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.concurrency.RequestExecutor;
import com.yelp.nrtsearch.server.luceneserver.facet.FacetTopDocs;
import com.yelp.nrtsearch.server.luceneserver.innerhit.InnerHitFetchTask;
import com.yelp.nrtsearch.server.luceneserver.rescore.RescoreTask;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper.CollectionTimeoutException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSidewaysImpl;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchV3Handler {
  private static final Logger logger = LoggerFactory.getLogger(SearchV3Handler.class);

  private SearchV3Handler() {}

  public static void executeSearch(
      IndexState indexState,
      SearchRequest searchRequest,
      RequestExecutor<SearchResponse, Long> requestExecutor)
      throws IOException, ExecutionException, InterruptedException {
    // TODO make configurable
    int maxParallelism = 4;
    var diagnostics = SearchResponse.Diagnostics.newBuilder();

    final ProfileResult.Builder profileResultBuilder;
    if (searchRequest.getProfile()) {
      profileResultBuilder = ProfileResult.newBuilder();
    } else {
      profileResultBuilder = null;
    }

    SearchContext searchContext;
    try {
      searchContext = buildSearchContext(searchRequest, indexState, diagnostics);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    requestExecutor.addOnFinished(() -> SearchV3Handler.releaseContext(searchContext));

    long searchStartTime = System.nanoTime();

    if (!searchRequest.getFacetsList().isEmpty()) {
      if (!(searchContext.getQuery() instanceof DrillDownQuery)) {
        throw new IllegalArgumentException("Can only use DrillSideways on DrillDownQuery");
      }
      DrillDownQuery ddq = (DrillDownQuery) searchContext.getQuery();

      List<FacetResult> grpcFacetResults = new ArrayList<>();
      DrillSidewaysImpl drillS =
          new DrillSidewaysImpl(
              searchContext.getSearcherAndTaxonomy().searcher,
              indexState.getFacetsConfig(),
              searchContext.getSearcherAndTaxonomy().taxonomyReader,
              searchRequest.getFacetsList(),
              searchContext.getSearcherAndTaxonomy(),
              indexState,
              searchContext.getShardState(),
              searchContext.getQueryFields(),
              grpcFacetResults,
              null,
              diagnostics);
      try {
        drillS.search(
            ddq,
            searchContext.getCollector().getWrappedManager(),
            r -> {
              try {
                postFacetRecall(
                    grpcFacetResults,
                    requestExecutor,
                    searchRequest,
                    searchContext,
                    diagnostics,
                    profileResultBuilder,
                    searchStartTime,
                    r);
              } catch (IOException | ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
              }
            },
            requestExecutor,
            maxParallelism);
      } catch (RuntimeException | IOException e) {
        // Searching with DrillSideways wraps exceptions in a few layers.
        // Try to find if this was caused by a timeout, if so, re-wrap
        // so that the top level exception is the same as when not using facets.
        CollectionTimeoutException timeoutException = findTimeoutException(e);
        if (timeoutException != null) {
          throw new CollectionTimeoutException(timeoutException.getMessage(), e);
        }
        throw e;
      }
    } else {
      ((MyIndexSearcher) searchContext.getSearcherAndTaxonomy().searcher)
          .search(
              searchContext.getQuery(),
              searchContext.getCollector().getWrappedManager(),
              r -> {
                try {
                  postRecall(
                      requestExecutor,
                      searchRequest,
                      searchContext,
                      diagnostics,
                      profileResultBuilder,
                      searchStartTime,
                      r);
                } catch (IOException | ExecutionException | InterruptedException e) {
                  throw new RuntimeException(e);
                }
              },
              requestExecutor,
              maxParallelism);
    }
  }

  private static void postFacetRecall(
      List<FacetResult> grpcFacetResults,
      RequestExecutor<SearchResponse, Long> requestExecutor,
      SearchRequest searchRequest,
      SearchContext searchContext,
      Diagnostics.Builder diagnostics,
      ProfileResult.Builder profileResultBuilder,
      long searchStartTime,
      SearcherResult searcherResult)
      throws IOException, ExecutionException, InterruptedException {
    searchContext.getResponseBuilder().addAllFacetResult(grpcFacetResults);
    searchContext
        .getResponseBuilder()
        .addAllFacetResult(
            FacetTopDocs.facetTopDocsSample(
                searcherResult.getTopDocs(),
                searchRequest.getFacetsList(),
                searchContext.getIndexState(),
                searchContext.getSearcherAndTaxonomy().searcher,
                diagnostics));
    postRecall(
        requestExecutor,
        searchRequest,
        searchContext,
        diagnostics,
        profileResultBuilder,
        searchStartTime,
        searcherResult);
  }

  private static void postRecall(
      RequestExecutor<SearchResponse, Long> requestExecutor,
      SearchRequest searchRequest,
      SearchContext searchContext,
      Diagnostics.Builder diagnostics,
      ProfileResult.Builder profileResultBuilder,
      long searchStartTime,
      SearcherResult searcherResult)
      throws IOException, ExecutionException, InterruptedException {
    TopDocs hits = searcherResult.getTopDocs();

    List<Explanation> explanations = null;
    if (searchRequest.getExplain()) {
      explanations = new ArrayList<>();
      for (ScoreDoc doc : hits.scoreDocs) {
        explanations.add(
            searchContext
                .getSearcherAndTaxonomy()
                .searcher
                .explain(searchContext.getQuery(), doc.doc));
      }
    }

    // add results from any extra collectors
    searchContext.getResponseBuilder().putAllCollectorResults(searcherResult.getCollectorResults());

    searchContext.getResponseBuilder().setHitTimeout(searchContext.getCollector().hadTimeout());
    searchContext
        .getResponseBuilder()
        .setTerminatedEarly(searchContext.getCollector().terminatedEarly());

    diagnostics.setFirstPassSearchTimeMs(((System.nanoTime() - searchStartTime) / 1000000.0));

    DeadlineUtils.checkDeadline("SearchHandler: post recall", "SEARCH");

    // add detailed timing metrics for query execution
    if (profileResultBuilder != null) {
      searchContext.getCollector().maybeAddProfiling(profileResultBuilder);
    }

    long rescoreStartTime = System.nanoTime();

    if (!searchContext.getRescorers().isEmpty()) {
      for (RescoreTask rescorer : searchContext.getRescorers()) {
        long startNS = System.nanoTime();
        hits = rescorer.rescore(hits, searchContext);
        long endNS = System.nanoTime();
        diagnostics.putRescorersTimeMs(rescorer.getName(), (endNS - startNS) / 1000000.0);
        DeadlineUtils.checkDeadline("SearchHandler: post " + rescorer.getName(), "SEARCH");
      }
      diagnostics.setRescoreTimeMs(((System.nanoTime() - rescoreStartTime) / 1000000.0));
    }

    long t0 = System.nanoTime();

    hits = getHitsFromOffset(hits, searchContext.getStartHit(), searchContext.getTopHits());

    // create Hit.Builder for each hit, and populate with lucene doc id and ranking info
    setResponseHits(searchContext, hits);

    // fill Hit.Builder with requested fields
    fetchFields(searchContext);

    SearchState.Builder searchState = SearchState.newBuilder();
    searchContext.getResponseBuilder().setSearchState(searchState);
    searchState.setTimestamp(searchContext.getTimestampSec());

    // Record searcher version that handled this request:
    searchState.setSearcherVersion(
        ((DirectoryReader) searchContext.getSearcherAndTaxonomy().searcher.getIndexReader())
            .getVersion());

    // Fill in lastDoc for searchAfter:
    if (hits.scoreDocs.length != 0) {
      ScoreDoc lastHit = hits.scoreDocs[hits.scoreDocs.length - 1];
      searchState.setLastDocId(lastHit.doc);
      searchContext.getCollector().fillLastHit(searchState, lastHit);
    }
    searchContext.getResponseBuilder().setSearchState(searchState);

    diagnostics.setGetFieldsTimeMs(((System.nanoTime() - t0) / 1000000.0));

    if (searchContext.getFetchTasks().getHighlightFetchTask() != null) {
      diagnostics.setHighlightTimeMs(
          searchContext.getFetchTasks().getHighlightFetchTask().getTimeTakenMs());
    }
    if (searchContext.getFetchTasks().getInnerHitFetchTaskList() != null) {
      diagnostics.putAllInnerHitsDiagnostics(
          searchContext.getFetchTasks().getInnerHitFetchTaskList().stream()
              .collect(
                  Collectors.toMap(
                      task -> task.getInnerHitContext().getInnerHitName(),
                      InnerHitFetchTask::getDiagnostic)));
    }
    searchContext.getResponseBuilder().setDiagnostics(diagnostics);

    if (profileResultBuilder != null) {
      searchContext.getResponseBuilder().setProfileResult(profileResultBuilder);
    }
    requestExecutor.addResult(searchContext.getResponseBuilder().build());
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
