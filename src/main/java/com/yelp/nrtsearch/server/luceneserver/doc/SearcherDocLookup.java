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
package com.yelp.nrtsearch.server.luceneserver.doc;

import com.yelp.nrtsearch.server.luceneserver.IndexState;
import org.apache.lucene.index.LeafReaderContext;

public class SearcherDocLookup extends DocLookup {

  private ThreadLocal<SegmentDocLookup> threadLookup = new ThreadLocal<>();

  public SearcherDocLookup(IndexState indexState) {
    super(indexState);
  }

  @Override
  public SegmentDocLookup getSegmentLookup(LeafReaderContext context) {
    SegmentDocLookup lookup = threadLookup.get();
    if (lookup == null) {
      // System.out.println("new context: " + context);
      lookup = new SegmentDocLookup(getIndexState(), context);
      threadLookup.set(lookup);
    } else {
      // System.out.println("set context: " + context);
      lookup.setContext(context);
    }
    return lookup;
  }
}
