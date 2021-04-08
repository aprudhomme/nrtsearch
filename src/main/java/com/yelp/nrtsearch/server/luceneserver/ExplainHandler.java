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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.ExplainRequest;
import com.yelp.nrtsearch.server.grpc.ExplainResponse;
import com.yelp.nrtsearch.server.luceneserver.field.IdFieldDef;
import java.io.IOException;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExplainHandler implements Handler<ExplainRequest, ExplainResponse> {
  private static final Logger logger = LoggerFactory.getLogger(ExplainHandler.class);
  private static final QueryNodeMapper nodeMapper = new QueryNodeMapper();

  @Override
  public ExplainResponse handle(IndexState indexState, ExplainRequest explainRequest)
      throws ExplainHandlerException {
    indexState.verifyStarted();

    IdFieldDef idFieldDef = indexState.getIdFieldDef();
    if (idFieldDef == null) {
      throw new ExplainHandlerException("Explain can only be used on indices with an _ID field.");
    }
    Query query = nodeMapper.getQuery(explainRequest.getQuery(), indexState);

    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    ExplainResponse.Builder responseBuilder = ExplainResponse.newBuilder();
    try {
      s = shardState.acquire();
      shardState.slm.record(s.searcher);

      TopDocs topDocs =
          s.searcher.search(
              new TermQuery(new Term(idFieldDef.getName(), explainRequest.getId())), 1);
      if (topDocs.scoreDocs.length == 0) {
        throw new ExplainHandlerException(
            String.format("No document found for id: %s", explainRequest.getId()));
      }
      int luceneDocId = topDocs.scoreDocs[0].doc;
      Explanation explanation = s.searcher.explain(query, luceneDocId);
      responseBuilder.setMatched(explanation.isMatch());
      responseBuilder.setExplanation(explanationToGrpc(explanation));
    } catch (IOException e) {
      throw new ExplainHandlerException(e);
    } finally {
      try {
        if (s != null) {
          shardState.release(s);
        }
      } catch (IOException e) {
        logger.warn("Failed to release searcher reference previously acquired by acquire()", e);
        throw new ExplainHandlerException(e);
      }
    }
    return responseBuilder.build();
  }

  private ExplainResponse.Explanation explanationToGrpc(Explanation explanation) {
    ExplainResponse.Explanation.Builder grpcExplanationBuilder =
        ExplainResponse.Explanation.newBuilder();
    grpcExplanationBuilder.setValue(explanationValueToGrpc(explanation.getValue()));
    grpcExplanationBuilder.setDescription(explanation.getDescription());
    for (Explanation e : explanation.getDetails()) {
      grpcExplanationBuilder.addDetails(explanationToGrpc(e));
    }
    return grpcExplanationBuilder.build();
  }

  private ExplainResponse.ExplanationValue explanationValueToGrpc(Number value) {
    ExplainResponse.ExplanationValue.Builder builder =
        ExplainResponse.ExplanationValue.newBuilder();
    if (value instanceof Integer) {
      builder.setIntValue(value.intValue());
    } else if (value instanceof Long) {
      builder.setLongValue(value.longValue());
    } else if (value instanceof Float) {
      builder.setFloatValue(value.floatValue());
    } else {
      builder.setDoubleValue(value.doubleValue());
    }
    return builder.build();
  }

  public static class ExplainHandlerException extends HandlerException {

    public ExplainHandlerException(Throwable err) {
      super(err);
    }

    public ExplainHandlerException(String message) {
      super(message);
    }

    public ExplainHandlerException(String message, Throwable err) {
      super(message, err);
    }
  }
}
