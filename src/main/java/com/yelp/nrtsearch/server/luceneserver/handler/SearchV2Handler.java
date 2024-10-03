/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.handler;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat.Printer;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.utils.ProtoMessagePrinter;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchV2Handler extends Handler<SearchRequest, Any> {
  private static final Logger logger = LoggerFactory.getLogger(SearchV2Handler.class.getName());
  private static final Printer protoMessagePrinter =
      ProtoMessagePrinter.omittingInsignificantWhitespace();

  private final SearchHandler searchHandler;

  public SearchV2Handler(GlobalState globalState, SearchHandler searchHandler) {
    super(globalState);
    this.searchHandler = searchHandler;
  }

  @Override
  public void handle(SearchRequest searchRequest, StreamObserver<Any> responseObserver) {
    try {
      SearchResponse searchResponse = searchHandler.getSearchResponse(searchRequest);
      setResponseCompression(searchRequest.getResponseCompression(), responseObserver);
      responseObserver.onNext(Any.pack(searchResponse));
      responseObserver.onCompleted();
    } catch (IOException e) {
      logger.warn(
          "error while trying to read index state dir for indexName: {}",
          searchRequest.getIndexName(),
          e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  "error while trying to read index state dir for indexName: "
                      + searchRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      String searchRequestJson = null;
      try {
        searchRequestJson = protoMessagePrinter.print(searchRequest);
      } catch (InvalidProtocolBufferException ignored) {
        // Ignore as invalid proto would have thrown an exception earlier
      }
      logger.warn(
          String.format(
              "error while trying to execute search for index %s: request: %s",
              searchRequest.getIndexName(), searchRequestJson),
          e);
      if (e instanceof StatusRuntimeException) {
        responseObserver.onError(e);
      } else {
        responseObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    String.format(
                        "error while trying to execute search for index %s. check logs for full searchRequest.",
                        searchRequest.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }
  }
}
