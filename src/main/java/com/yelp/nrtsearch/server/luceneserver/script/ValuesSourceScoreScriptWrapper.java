package com.yelp.nrtsearch.server.luceneserver.script;

import com.yelp.nrtsearch.server.luceneserver.doc.DocLookup;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;

import java.io.IOException;

public class ValuesSourceScoreScriptWrapper implements ScoreScript.SegmentFactory {
    private final DoubleValuesSource valuesSource;

    public ValuesSourceScoreScriptWrapper(DoubleValuesSource valuesSource) {
        this.valuesSource = valuesSource;
    }

    @Override
    public ScoreScript newInstance(LeafReaderContext context, DoubleValues scores) {
        try {
            return new ValuesScoreScriptWrapper(context, valuesSource.getValues(context, scores));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean needs_score() {
        return valuesSource.needsScores();
    }

    public static class ValuesScoreScriptWrapper extends ScoreScript {
        private final DoubleValues values;

        public ValuesScoreScriptWrapper(LeafReaderContext context, DoubleValues values) {
            super(null, new DocLookup(null, name -> null), context, values);
            this.values = values;
        }

        @Override
        public void setDocId(int doc) {
            try {
                values.advanceExact(doc);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public double execute() {
            try {
                return values.doubleValue();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
