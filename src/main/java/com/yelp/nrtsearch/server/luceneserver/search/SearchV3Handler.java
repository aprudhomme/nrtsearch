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
import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper.CollectionTimeoutException;
import com.yelp.nrtsearch.server.monitoring.SearchResponseCollector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSidewaysImpl;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
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
      RequestExecutor<?, SearchResponse, Long> requestExecutor)
      throws IOException, ExecutionException, InterruptedException {
    int maxRecallParallelism = indexState.getDefaultRecallParallelism();
    int maxFetchParallelism = indexState.getDefaultFetchParallelism();
    var diagnostics = SearchResponse.Diagnostics.newBuilder();

    final ProfileResult.Builder profileResultBuilder;
    if (searchRequest.getProfile()) {
      profileResultBuilder = ProfileResult.newBuilder();
    } else {
      profileResultBuilder = null;
    }
    requestExecutor.addOnFinished(
        t -> {
          if (t == null && indexState.getWarmer() != null) {
            indexState.getWarmer().addSearchRequest(searchRequest);
          }
        });

    SearchContext searchContext;
    try {
      searchContext =
          buildSearchContext(searchRequest, indexState, diagnostics, profileResultBuilder);
    } catch (RuntimeException re) {
      throw re;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    requestExecutor.addOnFinished(t -> SearchV3Handler.releaseContext(searchContext));

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
                    maxFetchParallelism,
                    searchStartTime,
                    r);
              } catch (IOException | ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
              }
            },
            requestExecutor,
            maxRecallParallelism);
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
                      searchContext,
                      diagnostics,
                      profileResultBuilder,
                      maxFetchParallelism,
                      searchStartTime,
                      r);
                } catch (IOException | ExecutionException | InterruptedException e) {
                  throw new RuntimeException(e);
                }
              },
              requestExecutor,
              maxRecallParallelism);
    }
  }

  private static void postFacetRecall(
      List<FacetResult> grpcFacetResults,
      RequestExecutor<?, SearchResponse, Long> requestExecutor,
      SearchRequest searchRequest,
      SearchContext searchContext,
      Diagnostics.Builder diagnostics,
      ProfileResult.Builder profileResultBuilder,
      int maxFetchParallelism,
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
        searchContext,
        diagnostics,
        profileResultBuilder,
        maxFetchParallelism,
        searchStartTime,
        searcherResult);
  }

  private static void postRecall(
      RequestExecutor<?, SearchResponse, Long> requestExecutor,
      SearchContext searchContext,
      Diagnostics.Builder diagnostics,
      ProfileResult.Builder profileResultBuilder,
      int maxFetchParallelism,
      long searchStartTime,
      SearcherResult searcherResult)
      throws IOException, ExecutionException, InterruptedException {
    TopDocs hits = searcherResult.getTopDocs();

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
      searchContext
          .getRescorers()
          .get(0)
          .rescore(
              hits,
              searchContext,
              requestExecutor,
              new RescorerConsumer(
                  rescoreStartTime,
                  System.nanoTime(),
                  0,
                  searchContext,
                  diagnostics,
                  profileResultBuilder,
                  maxFetchParallelism,
                  requestExecutor));
    } else {
      executeFetch(
          hits,
          searchContext,
          diagnostics,
          profileResultBuilder,
          maxFetchParallelism,
          requestExecutor);
    }
  }

  private static class RescorerConsumer implements Consumer<TopDocs> {
    private final long rescorePhaseStart;
    private final long rescorerStart;
    private final int index;
    private final SearchContext searchContext;
    private final Diagnostics.Builder diagnostics;
    private final ProfileResult.Builder profileResultBuilder;
    private final int maxFetchParallelism;
    private final RequestExecutor<?, SearchResponse, Long> requestExecutor;

    RescorerConsumer(
        long rescorePhaseStart,
        long rescorerStart,
        int index,
        SearchContext searchContext,
        Diagnostics.Builder diagnostics,
        ProfileResult.Builder profileResultBuilder,
        int maxFetchParallelism,
        RequestExecutor<?, SearchResponse, Long> requestExecutor) {
      this.rescorePhaseStart = rescorePhaseStart;
      this.rescorerStart = rescorerStart;
      this.index = index;
      this.searchContext = searchContext;
      this.diagnostics = diagnostics;
      this.profileResultBuilder = profileResultBuilder;
      this.maxFetchParallelism = maxFetchParallelism;
      this.requestExecutor = requestExecutor;
    }

    @Override
    public void accept(TopDocs topDocs) {
      String rescorerName = searchContext.getRescorers().get(index).getName();
      diagnostics.putRescorersTimeMs(rescorerName, (System.nanoTime() - rescorerStart) / 1000000.0);
      DeadlineUtils.checkDeadline("SearchHandler: post " + rescorerName, "SEARCH");

      int nextIndex = index + 1;
      if (nextIndex == searchContext.getRescorers().size()) {
        diagnostics.setRescoreTimeMs(((System.nanoTime() - rescorePhaseStart) / 1000000.0));
        try {
          executeFetch(
              topDocs,
              searchContext,
              diagnostics,
              profileResultBuilder,
              maxFetchParallelism,
              requestExecutor);
        } catch (IOException | ExecutionException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      } else {
        try {
          searchContext
              .getRescorers()
              .get(nextIndex)
              .rescore(
                  topDocs,
                  searchContext,
                  requestExecutor,
                  new RescorerConsumer(
                      rescorePhaseStart,
                      System.nanoTime(),
                      nextIndex,
                      searchContext,
                      diagnostics,
                      profileResultBuilder,
                      maxFetchParallelism,
                      requestExecutor));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static void executeFetch(
      TopDocs hits,
      SearchContext searchContext,
      Diagnostics.Builder diagnostics,
      ProfileResult.Builder profileResultBuilder,
      int maxFetchParallelism,
      RequestExecutor<?, SearchResponse, Long> requestExecutor)
      throws IOException, ExecutionException, InterruptedException {
    long t0 = System.nanoTime();

    final TopDocs finalHits =
        getHitsFromOffset(hits, searchContext.getStartHit(), searchContext.getTopHits());

    // create Hit.Builder for each hit, and populate with lucene doc id and ranking info
    setResponseHits(searchContext, finalHits);

    // fill Hit.Builder with requested fields
    fetchFields(
        searchContext,
        requestExecutor,
        () ->
            finishSearch(
                finalHits, searchContext, diagnostics, profileResultBuilder, t0, requestExecutor),
        maxFetchParallelism);
  }

  private static void finishSearch(
      TopDocs hits,
      SearchContext searchContext,
      Diagnostics.Builder diagnostics,
      ProfileResult.Builder profileResultBuilder,
      long fetchStartNs,
      RequestExecutor<?, SearchResponse, Long> requestExecutor) {
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

    diagnostics.setGetFieldsTimeMs(((System.nanoTime() - fetchStartNs) / 1000000.0));

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
    SearchResponse searchResponse = searchContext.getResponseBuilder().build();
    SearchResponseCollector.updateSearchResponseMetrics(
        searchResponse,
        searchContext.getIndexState().getName(),
        searchContext.getIndexState().getVerboseMetrics());
    requestExecutor.addResult(searchContext.getResponseBuilder().build());
  }

  private static SearchContext buildSearchContext(
      SearchRequest searchRequest,
      IndexState indexState,
      Diagnostics.Builder diagnostics,
      ProfileResult.Builder profileResultBuilder)
      throws IOException, InterruptedException {
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = getSearcherAndTaxonomy(searchRequest, indexState, shardState, diagnostics, null);
      return SearchRequestProcessor.buildContextForRequest(
          searchRequest, indexState, shardState, s, profileResultBuilder);
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
