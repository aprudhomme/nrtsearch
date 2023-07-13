package org.apache.lucene.search;

import com.yelp.nrtsearch.server.luceneserver.search.NotifyCollectionCompleted;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;

/**
 * A {@link CollectorManager} implements which wrap a set of {@link CollectorManager}
 * as {@link MultiCollector} acts for {@link Collector}.
 */
public class MultiCollectorManager implements CollectorManager<MultiCollectorManager.Collectors, Object[]> {

  final private CollectorManager<Collector, ?>[] collectorManagers;

  @SafeVarargs
  @SuppressWarnings({"varargs", "unchecked"})
  public MultiCollectorManager(final CollectorManager<? extends Collector, ?>... collectorManagers) {
    if (collectorManagers.length < 1) {
      throw new IllegalArgumentException("There must be at least one collector");
    }
    this.collectorManagers = (CollectorManager[]) collectorManagers;
  }

  @Override
  public Collectors newCollector() throws IOException {
    return new Collectors();
  }

  @Override
  public Object[] reduce(Collection<Collectors> reducableCollectors) throws IOException {
    final int size = reducableCollectors.size();
    final Object[] results = new Object[collectorManagers.length];
    for (int i = 0; i < collectorManagers.length; i++) {
      final List<Collector> reducableCollector = new ArrayList<>(size);
      for (Collectors collectors : reducableCollectors)
        reducableCollector.add(collectors.collectors[i]);
      results[i] = collectorManagers[i].reduce(reducableCollector);
    }
    return results;
  }

  public class Collectors implements Collector, NotifyCollectionCompleted {

    private final Collector[] collectors;

    private Collectors() throws IOException {
      collectors = new Collector[collectorManagers.length];
      for (int i = 0; i < collectors.length; i++)
        collectors[i] = collectorManagers[i].newCollector();
    }

    @Override
    final public LeafCollector getLeafCollector(final LeafReaderContext context) throws IOException {
      return new LeafCollectors(context);
    }

    @Override
    final public ScoreMode scoreMode() {
      ScoreMode scoreMode = null;
      for (Collector collector : collectors) {
        if (scoreMode == null) {
          scoreMode = collector.scoreMode();
        } else if (scoreMode != collector.scoreMode()) {
          return ScoreMode.COMPLETE;
        }
      }
      return scoreMode;
    }

    @Override
    public void collectionCompleted() {
      for (Collector collector : collectors) {
        NotifyCollectionCompleted.maybeNotify(collector);
      }
    }

    public class LeafCollectors implements LeafCollector {

      private final LeafCollector[] leafCollectors;

      private LeafCollectors(final LeafReaderContext context) throws IOException {
        leafCollectors = new LeafCollector[collectors.length];
        for (int i = 0; i < collectors.length; i++)
          leafCollectors[i] = collectors[i].getLeafCollector(context);
      }

      @Override
      final public void setScorer(final Scorable scorer) throws IOException {
        for (LeafCollector leafCollector : leafCollectors)
          if (leafCollector != null)
            leafCollector.setScorer(scorer);
      }

      @Override
      final public void collect(final int doc) throws IOException {
        for (LeafCollector leafCollector : leafCollectors)
          if (leafCollector != null)
            leafCollector.collect(doc);
      }
    }
  }

}
