package com.yelp.nrtsearch.server.luceneserver.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Objects;

public class ScoreScriptValueSourceWrapper extends DoubleValuesSource {
    private final ScoreScript.SegmentFactory factory;

    public ScoreScriptValueSourceWrapper(ScoreScript.SegmentFactory factory) {
        this.factory = factory;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        ScoreScript script = factory.newInstance(ctx, scores);
        return new ScoreScriptDoubleValuesWrapper(script);
    }

    @Override
    public boolean needsScores() {
        return factory.needs_score();
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(factory);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
        return false;
      }
      if (obj.getClass() != this.getClass()) {
        return false;
      }
      ScoreScriptValueSourceWrapper other = (ScoreScriptValueSourceWrapper) obj;
      return Objects.equals(other.factory, this.factory);
    }

    @Override
    public String toString() {
        return "ScoreScriptValueSourceWrapper(" + factory + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    public static class ScoreScriptDoubleValuesWrapper extends DoubleValues {
        private final ScoreScript script;

        public ScoreScriptDoubleValuesWrapper(ScoreScript script) {
            this.script = script;
        }

        @Override
        public double doubleValue() throws IOException {
            return script.execute();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            script.setDocId(target);
            return true;
        }
    }
}
