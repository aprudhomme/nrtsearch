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
package com.yelp.nrtsearch.server.luceneserver.search.fetch;

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;

/** Class that manages the execution of custom {@link FetchTask}s. */
public class FetchTasks {

  /**
   * Interface for a custom task that should be run while fetching field values. There are three
   * separate methods that may be optionally implemented. The order of operations is as follows:
   *
   * <p>1) All the documents in a segment have their query specified fields filled
   *
   * <p>2) For each {@link FetchTask} in order:
   *
   * <p>2a) The {@link FetchTask#processSegmentHits(SearchContext, LeafReaderContext, List)} method
   * is called for the segment
   *
   * <p>2b) The {@link FetchTask#processHit(SearchContext, LeafReaderContext,
   * SearchResponse.Hit.Builder)} method is called for each segment document
   *
   * <p>3) The {@link FetchTask#processAllHits(SearchContext, List)} method is called on each {@link
   * FetchTask} in order
   */
  public interface FetchTask {
    /**
     * Process the list of all query hits. This is the final fetch operation.
     *
     * @param searchContext search context
     * @param hits query hits in lucene doc id order
     * @throws IOException on error reading data
     */
    default void processAllHits(SearchContext searchContext, List<SearchResponse.Hit.Builder> hits)
        throws IOException {}

    /**
     * Process all the query hits for each segment. Hits will already have lucene doc id and scoring
     * info populated, as well as all required query fields.
     *
     * @param searchContext search context
     * @param segmentLeaf lucene segment containing hits
     * @param segmentHits all query hits in this segment, in lucene doc id order
     * @throws IOException on error reading data
     */
    default void processSegmentHits(
        SearchContext searchContext,
        LeafReaderContext segmentLeaf,
        List<SearchResponse.Hit.Builder> segmentHits)
        throws IOException {}

    /**
     * Process each hit individually. Hits will already have lucene doc id and scoring info
     * populated, as well as all required query fields. Runs after {@link
     * FetchTask#processSegmentHits(SearchContext, LeafReaderContext, List)}.
     *
     * @param searchContext search context
     * @param hitLeaf lucene segment for hit
     * @param hit query hit
     * @throws IOException on error reading data
     */
    default void processHit(
        SearchContext searchContext, LeafReaderContext hitLeaf, SearchResponse.Hit.Builder hit)
        throws IOException {}
  }

  private final List<FetchTask> taskList;

  /**
   * Constructor.
   *
   * @param grpcTaskList fetch task definitions from search request
   */
  public FetchTasks(List<com.yelp.nrtsearch.server.grpc.FetchTask> grpcTaskList) {
    taskList =
        grpcTaskList.stream()
            .map(t -> FetchTaskCreator.getInstance().createFetchTask(t))
            .collect(Collectors.toList());
  }

  /**
   * Invoke the {@link FetchTask#processAllHits(SearchContext, List)} method on all query {@link
   * FetchTask}s.
   *
   * @param searchContext search context
   * @param hits list of query hits in lucene doc id order
   * @throws IOException on error reading data
   */
  public void processAllHits(SearchContext searchContext, List<SearchResponse.Hit.Builder> hits)
      throws IOException {
    for (FetchTask task : taskList) {
      task.processAllHits(searchContext, hits);
    }
  }

  /**
   * Invoke the {@link FetchTask#processSegmentHits(SearchContext, LeafReaderContext, List)}
   * followed by the {@link FetchTask#processHit(SearchContext, LeafReaderContext,
   * SearchResponse.Hit.Builder)} methods for each query {@link FetchTask}.
   *
   * @param searchContext search context
   * @param segmentLeaf lucene segment for hits
   * @param segmentHits segment hits
   * @throws IOException on error reading data
   */
  public void processSegmentHits(
      SearchContext searchContext,
      LeafReaderContext segmentLeaf,
      List<SearchResponse.Hit.Builder> segmentHits)
      throws IOException {
    for (FetchTask task : taskList) {
      task.processSegmentHits(searchContext, segmentLeaf, segmentHits);
      for (var hit : segmentHits) {
        task.processHit(searchContext, segmentLeaf, hit);
      }
    }
  }
}
