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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper;
import com.yelp.nrtsearch.server.luceneserver.search.SearchStatsWrapper;
import com.yelp.nrtsearch.server.luceneserver.search.TerminateAfterWrapper;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;
import org.mockito.Mockito;

public class DocCollectorTest {

  private static class TestDocCollector extends DocCollector {
    private final CollectorManager<? extends Collector, ? extends TopDocs> manager;

    public static IndexState getMockState() {
      IndexState indexState = Mockito.mock(IndexState.class);
      when(indexState.getDefaultSearchTimeoutSec()).thenReturn(0.0);
      when(indexState.getDefaultSearchTimeoutCheckEvery()).thenReturn(0);
      return indexState;
    }

    public TestDocCollector(
        int hitsToCollect,
        double searchTimeoutSec,
        int searchTimeoutCheckEvery,
        int searchTerminateAfter,
        boolean profile) {
      this(
          hitsToCollect,
          searchTimeoutSec,
          searchTimeoutCheckEvery,
          searchTerminateAfter,
          profile,
          getMockState());
    }

    public TestDocCollector(
        int hitsToCollect,
        double searchTimeoutSec,
        int searchTimeoutCheckEvery,
        int searchTerminateAfter,
        boolean profile,
        IndexState indexState) {
      super(
          new CollectorCreatorContext(
              hitsToCollect,
              searchTimeoutSec,
              searchTimeoutCheckEvery,
              searchTerminateAfter,
              false,
              profile,
              indexState.docLookup,
              Collections.emptyMap(),
              null),
          Collections.emptyList());
      manager = new TestCollectorManager();
    }

    @Override
    public CollectorManager<? extends Collector, ? extends TopDocs> getManager() {
      return manager;
    }

    @Override
    public void fillHitRanking(Builder hitResponse, ScoreDoc scoreDoc) {}

    @Override
    public void fillLastHit(SearchState.Builder stateBuilder, ScoreDoc lastHit) {}

    public static class TestCollectorManager implements CollectorManager<Collector, TopDocs> {

      @Override
      public Collector newCollector() throws IOException {
        return null;
      }

      @Override
      public TopDocs reduce(Collection<Collector> collectors) throws IOException {
        return null;
      }
    }
  }

  @Test
  public void testNoTimeoutWrapper() {
    TestDocCollector docCollector = new TestDocCollector(10, 0, 0, 0, false);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchCollectorManager);
    assertSame(
        docCollector.getManager(),
        ((SearchCollectorManager) docCollector.getWrappedManager()).getDocCollectorManger());
  }

  @Test
  public void testHasTimeoutWrapper() {
    TestDocCollector docCollector = new TestDocCollector(10, 5, 0, 0, false);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchCutoffWrapper);
    SearchCutoffWrapper<?> cutoffWrapper =
        (SearchCutoffWrapper<?>) docCollector.getWrappedManager();
    assertEquals(5.0, cutoffWrapper.getTimeoutSec(), 0.0);
    assertEquals(0, cutoffWrapper.getCheckEvery());
  }

  @Test
  public void testHasTimeoutWrapperCheckEvery() {
    TestDocCollector docCollector = new TestDocCollector(10, 5, 100, 0, false);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchCutoffWrapper);
    SearchCutoffWrapper<?> cutoffWrapper =
        (SearchCutoffWrapper<?>) docCollector.getWrappedManager();
    assertEquals(5.0, cutoffWrapper.getTimeoutSec(), 0.0);
    assertEquals(100, cutoffWrapper.getCheckEvery());
  }

  @Test
  public void testHasStatsWrapper() {
    TestDocCollector docCollector = new TestDocCollector(10, 0, 0, 0, true);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchStatsWrapper);
    SearchStatsWrapper<?> searchStatsWrapper =
        (SearchStatsWrapper<?>) docCollector.getWrappedManager();
    assertTrue(searchStatsWrapper.getWrapped() instanceof SearchCollectorManager);
    SearchCollectorManager searchCollectorManager =
        (SearchCollectorManager) searchStatsWrapper.getWrapped();
    assertSame(docCollector.getManager(), searchCollectorManager.getDocCollectorManger());
  }

  @Test
  public void testHasStatsAndTimeoutWrapper() {
    TestDocCollector docCollector = new TestDocCollector(10, 5, 0, 0, true);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchStatsWrapper);
    assertTrue(
        ((SearchStatsWrapper<?>) docCollector.getWrappedManager()).getWrapped()
            instanceof SearchCutoffWrapper);
  }

  @Test
  public void testHasTerminateAfterWrapper() {
    TestDocCollector docCollector = new TestDocCollector(10, 0, 0, 5, false);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof TerminateAfterWrapper);
    assertEquals(
        5, ((TerminateAfterWrapper<?>) docCollector.getWrappedManager()).getTerminateAfter());
  }

  @Test
  public void testWithAllWrappers() {
    TestDocCollector docCollector = new TestDocCollector(10, 5, 0, 3, true);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchStatsWrapper);
    SearchStatsWrapper<?> statsWrapper = (SearchStatsWrapper<?>) docCollector.getWrappedManager();
    assertTrue(statsWrapper.getWrapped() instanceof TerminateAfterWrapper);
    TerminateAfterWrapper<?> terminateAfterWrapper =
        (TerminateAfterWrapper<?>) statsWrapper.getWrapped();
    assertTrue(terminateAfterWrapper.getWrapped() instanceof SearchCutoffWrapper);
  }
}
