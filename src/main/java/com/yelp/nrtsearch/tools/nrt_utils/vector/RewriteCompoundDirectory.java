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
package com.yelp.nrtsearch.tools.nrt_utils.vector;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

public class RewriteCompoundDirectory extends Directory {
  private final Directory compoundDir;
  private final Directory vecDir;

  private boolean inCompoundFile(String name) throws IOException {
    if (IndexFileNames.matchesExtension(name, "vec")
        || IndexFileNames.matchesExtension(name, "vem")
        || IndexFileNames.matchesExtension(name, "vex")) {
      return false;
    }
    for (String file : compoundDir.listAll()) {
      if (file.equals(name)) {
        return true;
      }
    }
    return false;
  }

  public RewriteCompoundDirectory(Directory compoundDir, Directory vecDir) {
    this.compoundDir = compoundDir;
    this.vecDir = vecDir;
  }

  @Override
  public String[] listAll() throws IOException {
    Set<String> allFiles = new HashSet<>();
    Collections.addAll(allFiles, compoundDir.listAll());
    Collections.addAll(allFiles, vecDir.listAll());
    return allFiles.toArray(new String[0]);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    vecDir.deleteFile(name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    if (inCompoundFile(name)) {
      return compoundDir.fileLength(name);
    } else {
      return vecDir.fileLength(name);
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return vecDir.createOutput(name, context);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    return vecDir.createTempOutput(prefix, suffix, context);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    vecDir.sync(names);
  }

  @Override
  public void syncMetaData() throws IOException {
    vecDir.syncMetaData();
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    vecDir.rename(source, dest);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (inCompoundFile(name)) {
      return compoundDir.openInput(name, context);
    } else {
      return vecDir.openInput(name, context);
    }
  }

  @Override
  public Lock obtainLock(String name) throws IOException {
    if (inCompoundFile(name)) {
      return compoundDir.obtainLock(name);
    } else {
      return vecDir.obtainLock(name);
    }
  }

  @Override
  public void close() throws IOException {
    // Directories will be closed elsewhere
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    return vecDir.getPendingDeletions();
  }
}
