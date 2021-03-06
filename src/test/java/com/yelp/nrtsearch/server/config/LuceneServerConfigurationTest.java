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
package com.yelp.nrtsearch.server.config;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.util.Map;
import org.junit.Test;

public class LuceneServerConfigurationTest {

  private LuceneServerConfiguration getForConfig(String config) {
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  // A little hacky, but not many option for setting environment variables in a running process
  private void setEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field f = cl.getDeclaredField("m");
      f.setAccessible(true);
      Map<String, String> mutableEnv = (Map<String, String>) f.get(env);
      mutableEnv.put(key, value);
    } catch (Exception e) {
      throw new RuntimeException("Unable to set environment variable", e);
    }
  }

  @Test
  public void testGetsHostName() {
    String config = String.join("\n", "nodeName: \"lucene_server_foo\"", "hostName: my_host_name");
    LuceneServerConfiguration luceneConfig = getForConfig(config);
    assertEquals("my_host_name", luceneConfig.getHostName());
  }

  @Test
  public void testGetEnvHostName() {
    setEnv("CUSTOM_HOST", "my_custom_host");
    String config =
        String.join("\n", "nodeName: \"lucene_server_foo\"", "hostName: ${CUSTOM_HOST}");
    LuceneServerConfiguration luceneConfig = getForConfig(config);
    assertEquals("my_custom_host", luceneConfig.getHostName());
  }

  @Test
  public void testGetMultiEnvHostName() {
    setEnv("VAR1", "v1");
    setEnv("VAR2", "v2");
    String config =
        String.join(
            "\n", "nodeName: \"lucene_server_foo\"", "hostName: my_${VAR1}_${VAR2}_${VAR1}_host");
    LuceneServerConfiguration luceneConfig = getForConfig(config);
    assertEquals("my_v1_v2_v1_host", luceneConfig.getHostName());
  }

  @Test
  public void testMissingEnvHostName() {
    setEnv("VAR3", "v3");
    String config =
        String.join(
            "\n", "nodeName: \"lucene_server_foo\"", "hostName: my_${VAR4}_${VAR3}_${VAR4}_host");
    LuceneServerConfiguration luceneConfig = getForConfig(config);
    assertEquals("my__v3__host", luceneConfig.getHostName());
  }
}
