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
package com.yelp.nrtsearch.server.luceneserver.concurrency;

import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class RequestExecutor<T, U> {
  private final TaskExecutor<U> taskExecutor;
  private final StreamObserver<T> responseStreamObserver;
  private final U priority;
  private final ConcurrentLinkedDeque<Runnable> onFinished = new ConcurrentLinkedDeque<>();
  private final AtomicInteger pendingTasks = new AtomicInteger();
  private volatile Throwable t = null;

  public RequestExecutor(
      TaskExecutor<U> taskExecutor, StreamObserver<T> responseStreamObserver, U priority) {
    this.taskExecutor = taskExecutor;
    this.responseStreamObserver = responseStreamObserver;
    this.priority = priority;
    taskExecutor.startRequest();
    addOnFinished(taskExecutor::endRequest);
  }

  public synchronized void addResult(T result) {
    responseStreamObserver.onNext(result);
  }

  public void onError(Throwable error) {
    if (t != null) {
      t = error;
    }
    taskFinished();
  }

  private void taskFinished() {
    int pending = pendingTasks.decrementAndGet();
    if (pending == 0) {
      synchronized (this) {
        if (t != null) {
          responseStreamObserver.onError(t);
        } else {
          responseStreamObserver.onCompleted();
        }
      }
      for (Runnable finishTask : onFinished) {
        try {
          finishTask.run();
        } catch (Throwable t) {
          System.out.println("Ignored exception in finish task: " + t);
        }
      }
    }
  }

  public void addOnFinished(Runnable r) {
    onFinished.addFirst(r);
  }

  public void execute(Runnable task) {
    pendingTasks.incrementAndGet();
    taskExecutor.execute(task, priority, this::taskFinished, this::onError);
  }

  public void executeMultiAndThen(List<Runnable> tasks, Runnable nextTask, int maxParallelism) {
    pendingTasks.incrementAndGet();
    taskExecutor.executeMultiAndThen(
        tasks, priority, nextTask, maxParallelism, this::taskFinished, this::onError);
  }

  public <V> void executeMultiAndThen(
      List<Callable<V>> tasks, Consumer<List<V>> nextTask, int maxParallelism) {
    pendingTasks.incrementAndGet();
    taskExecutor.executeMultiAndThen(
        tasks, priority, nextTask, maxParallelism, this::taskFinished, this::onError);
  }
}
