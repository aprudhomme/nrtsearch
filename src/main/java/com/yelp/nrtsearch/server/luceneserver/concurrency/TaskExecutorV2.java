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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutorV2<T> {
  private static final Logger logger = LoggerFactory.getLogger(TaskExecutorV2.class);
  private final TaskThreadPool taskThreadPool;
  private final int maxActiveRequests;
  private final AtomicInteger activeRequests = new AtomicInteger();

  interface Task {
    void start();

    void taskCompleted(Runnable r, int id, Throwable t);
  }

  private class SingleTask implements Task {
    private final Runnable task;
    private final T priority;
    private final Runnable onComplete;
    private final Consumer<Throwable> onError;

    SingleTask(Runnable task, T priority, Runnable onComplete, Consumer<Throwable> onError) {
      this.task = task;
      this.priority = priority;
      this.onComplete = onComplete;
      this.onError = onError;
    }

    @Override
    public void start() {
      taskThreadPool.execute(new TaskRunnable<>(task, priority, this, 0));
    }

    @Override
    public void taskCompleted(Runnable r, int id, Throwable t) {
      if (t != null) {
        onError.accept(t);
      } else {
        onComplete.run();
      }
    }
  }

  private class MultiCallableTask<V> implements Task {
    private final List<Callable<V>> tasks;
    private final T priority;
    private final Consumer<List<V>> nextTask;
    private final int maxParallelism;
    private final Runnable onComplete;
    private final Consumer<Throwable> onError;
    private final List<V> resultsList;
    private final AtomicInteger startedSubTasks = new AtomicInteger();
    private final AtomicInteger completedSubTasks = new AtomicInteger();
    private volatile Throwable error = null;

    MultiCallableTask(
        List<Callable<V>> tasks,
        T priority,
        Consumer<List<V>> nextTask,
        int maxParallelism,
        Runnable onComplete,
        Consumer<Throwable> onError) {
      this.tasks = tasks;
      this.priority = priority;
      this.nextTask = nextTask;
      this.maxParallelism = maxParallelism;
      this.onComplete = onComplete;
      this.onError = onError;

      this.resultsList = new ArrayList<>(tasks.size());
      for (int i = 0; i < tasks.size(); ++i) {
        this.resultsList.add(null);
      }
    }

    @Override
    public void start() {
      int tasksToStart = Math.min(tasks.size(), maxParallelism);
      for (int i = 0; i < tasksToStart; ++i) {
        int id = startedSubTasks.getAndIncrement();
        if (id >= tasks.size()) {
          break;
        }
        taskThreadPool.execute(
            new TaskRunnable<>(new FutureTask<>(tasks.get(id)), priority, this, id));
      }
    }

    @Override
    public void taskCompleted(Runnable r, int id, Throwable t) {
      if (t != null) {
        if (error == null) {
          error = t;
        }
      } else {
        try {
          resultsList.add(id, ((FutureTask<V>) r).get());
        } catch (InterruptedException | ExecutionException e) {
          if (error == null) {
            error = e;
          }
        }
      }
      int currentCompletedSubTasks = completedSubTasks.incrementAndGet();
      if (currentCompletedSubTasks == tasks.size()) {
        if (error != null) {
          onError.accept(error);
        } else {
          execute(() -> nextTask.accept(resultsList), priority, onComplete, onError);
        }
      } else {
        int nextId = startedSubTasks.getAndIncrement();
        if (nextId < tasks.size()) {
          taskThreadPool.execute(
              new TaskRunnable<>(new FutureTask<>(tasks.get(id)), priority, this, id));
        }
      }
    }
  }

  private static class TaskRunnable<U> implements Runnable {
    private final Runnable wrapped;
    private final U priority;
    private final Task task;
    private final int id;

    TaskRunnable(Runnable wrapped, U priority, Task task, int id) {
      this.wrapped = wrapped;
      this.priority = priority;
      this.task = task;
      this.id = id;
    }

    @Override
    public void run() {
      wrapped.run();
    }

    public void taskCompleted(Throwable t) {
      task.taskCompleted(wrapped, id, t);
    }
  }

  public TaskExecutorV2(int numThreads, Comparator<T> queueComparator, int maxActiveRequests) {
    this.maxActiveRequests = maxActiveRequests;
    this.taskThreadPool =
        new TaskThreadPool(
            numThreads,
            numThreads,
            0,
            TimeUnit.SECONDS,
            new PriorityBlockingQueue<>(
                numThreads,
                Comparator.comparing(
                        runnable -> ((TaskRunnable<T>) runnable).priority, queueComparator)
                    .thenComparingInt(runnable -> ((TaskRunnable<T>) runnable).id)),
            (r, e) -> logger.error("Thread pool rejected task"));
  }

  public void startRequest() {
    int active = activeRequests.incrementAndGet();
    if (active > maxActiveRequests) {
      activeRequests.decrementAndGet();
      throw new RejectedExecutionException("More than " + maxActiveRequests + " active requests");
    }
  }

  public void endRequest() {
    activeRequests.decrementAndGet();
  }

  public void execute(Runnable r, T priority, Runnable onComplete, Consumer<Throwable> onError) {
    Task task = new SingleTask(r, priority, onComplete, onError);
    task.start();
  }

  public <V> void executeMultiAndThen(
      List<Callable<V>> tasks,
      T priority,
      Consumer<List<V>> nextTask,
      int maxParallelism,
      Runnable onComplete,
      Consumer<Throwable> onError) {
    Task task =
        new MultiCallableTask<V>(tasks, priority, nextTask, maxParallelism, onComplete, onError);
    task.start();
  }

  private static class TaskThreadPool extends ThreadPoolExecutor {
    public TaskThreadPool(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public void execute(TaskRunnable<?> taskRunnable) {
      super.execute(taskRunnable);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
      System.out.println("beforeExecute: thread: " + t + ", runnable: " + r);
      super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      System.out.println("afterExecute: runnable: " + r + ", throwable: " + t);
      TaskRunnable<?> taskRunnable = (TaskRunnable<?>) r;
      taskRunnable.taskCompleted(t);
    }

    @Override
    protected <U> RunnableFuture<U> newTaskFor(Callable<U> callable) {
      RunnableFuture<U> rf = super.newTaskFor(callable);
      System.out.println(
          "newTaskFor: callable: " + callable + ", runnableFuture: " + rf + ", wrapped: " + null);
      return null;
    }
  }
}
