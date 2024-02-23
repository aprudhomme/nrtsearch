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

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestExecutor<T, U, V> {
  private static final Logger logger = LoggerFactory.getLogger(RequestExecutor.class);
  private final TaskExecutor<V> taskExecutor;
  private final StreamObserver<T> responseStreamObserver;
  private final Function<U, T> responseFunc;
  private final V priority;
  private final Function<Throwable, StatusRuntimeException> exceptionFunc;
  private final ConcurrentLinkedDeque<OnFinishedTask> onFinished = new ConcurrentLinkedDeque<>();
  private final AtomicInteger pendingTasks = new AtomicInteger();
  private volatile Throwable t = null;

  @FunctionalInterface
  public interface OnFinishedTask {
    void execute(Throwable t);
  }

  public RequestExecutor(
      TaskExecutor<V> taskExecutor,
      StreamObserver<T> responseStreamObserver,
      Function<U, T> responseFunc,
      V priority,
      Function<Throwable, StatusRuntimeException> exceptionFunc) {
    this.taskExecutor = taskExecutor;
    this.responseStreamObserver = responseStreamObserver;
    this.responseFunc = responseFunc;
    this.priority = priority;
    this.exceptionFunc = exceptionFunc;
    taskExecutor.startRequest();
    addOnFinished(t -> taskExecutor.endRequest());
  }

  public synchronized void addResult(U result) {
    responseStreamObserver.onNext(responseFunc.apply(result));
  }

  public void onError(Throwable error) {
    if (t == null) {
      t = error;
    }
    taskFinished();
  }

  private void taskFinished() {
    int pending = pendingTasks.decrementAndGet();
    if (pending == 0) {
      try {
        synchronized (this) {
          if (t != null) {
            Throwable resultError = t;
            try {
              resultError = exceptionFunc.apply(t);
            } catch (Throwable funct) {
              logger.warn("Ignored exception from exception function: " + funct);
            }
            responseStreamObserver.onError(resultError);
          } else {
            responseStreamObserver.onCompleted();
          }
        }
      } finally {
        for (OnFinishedTask finishTask : onFinished) {
          try {
            finishTask.execute(t);
          } catch (Throwable t) {
            logger.warn("Ignored exception in finish task: " + t);
          }
        }
      }
    }
  }

  public void addOnFinished(OnFinishedTask task) {
    onFinished.addFirst(task);
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
