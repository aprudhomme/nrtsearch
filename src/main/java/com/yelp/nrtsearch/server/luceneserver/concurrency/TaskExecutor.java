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
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutor {
  private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);
  private final TaskThreadPool taskThreadPool;
  private final PriorityQueue<TaskGroup<?>> pendingTaskQueue;
  private final Comparator<TaskGroup<?>> queueComparator;
  private final int maxActiveTasks;
  private int activeTasks = 0;

  public TaskExecutor(int numThreads) {
    taskThreadPool =
        new TaskThreadPool(
            numThreads,
            numThreads,
            0,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(numThreads),
            (r, e) -> logger.error("Thread pool rejected task"));
    this.maxActiveTasks = numThreads;
    queueComparator = Comparator.comparingLong(tg -> tg.timestamp);
    pendingTaskQueue = new PriorityQueue<>(queueComparator);
  }

  /*public <T> Future<T> submit(Callable<T> task, long timestamp) {

  }*/

  public synchronized <T> Future<List<T>> submit(
      List<Callable<T>> tasks, long timestamp, int maxParallelism) {
    TaskGroup<T> taskGroup = new TaskGroup<>(tasks, timestamp, maxParallelism);
    int numNewTasks = taskGroup.startTasks(maxActiveTasks - activeTasks, taskThreadPool);
    activeTasks += numNewTasks;
    if (taskGroup.shouldBeQueued()) {
      pendingTaskQueue.add(taskGroup);
    }
    return taskGroup;
  }

  private synchronized void startQueuedTasks() {
    while (maxActiveTasks - activeTasks > 0 && !pendingTaskQueue.isEmpty()) {
      TaskGroup<?> taskGroup = pendingTaskQueue.peek();
      int newTasks = taskGroup.startTasks(maxActiveTasks - activeTasks, taskThreadPool);
      activeTasks += newTasks;
      if (!taskGroup.shouldBeQueued()) {
        pendingTaskQueue.poll();
      }
    }
  }

  private static class TaskGroup<T> implements Future<List<T>> {
    private final List<Callable<T>> tasks;
    private final List<Future<T>> futures = new ArrayList<>();
    private final long timestamp;
    private final int maxParallelism;
    private int activeTasks = 0;
    private int startedTasks = 0;
    private Throwable throwable;
    private boolean done = false;
    private List<T> results;

    TaskGroup(List<Callable<T>> tasks, long timestamp, int maxParallelism) {
      this.tasks = tasks;
      this.timestamp = timestamp;
      this.maxParallelism = maxParallelism;
    }

    int startTasks(int availableThreads, TaskThreadPool threadPool) {
      if (throwable != null) {
        return 0;
      }
      int numTasksToStart =
          Math.min(Math.min(tasks.size() - startedTasks, maxParallelism), availableThreads);
      System.out.println("Started tasks: " + numTasksToStart);
      for (int i = 0; i < numTasksToStart; ++i) {
        System.out.println("Submitting task: " + tasks.get(startedTasks));
        futures.add(threadPool.submit(tasks.get(startedTasks), this));
        System.out.println();
        activeTasks++;
        startedTasks++;
      }
      return numTasksToStart;
    }

    synchronized void taskCompleted(Throwable t) {
      if (t != null) {
        throwable = t;
      }
      activeTasks--;

      if (activeTasks == 0) {
        if (throwable != null || startedTasks == tasks.size()) {
            done = true;
            notifyAll();
        }
      }
    }

    boolean shouldBeQueued() {
      return throwable == null && startedTasks != tasks.size() && activeTasks != maxParallelism;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public synchronized boolean isDone() {
      return done;
    }

    @Override
    public synchronized List<T> get() throws InterruptedException, ExecutionException {
      while (!done) {
        wait();
      }
      return getResults();
    }

    @Override
    public synchronized List<T> get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      while (!done) {
        // TODO convert timeout to ms
        wait(timeout);
      }
      return getResults();
    }

    private List<T> getResults() throws ExecutionException, InterruptedException {
      if (throwable != null) {
        throw new ExecutionException(throwable);
      }
      if (results == null) {
        results = new ArrayList<>(tasks.size());
        for (Future<T> future : futures) {
          results.add(future.get());
        }
      }
      return results;
    }
  }

  private static class RunnableFutureWrapper<V> implements RunnableFuture<V> {
    final RunnableFuture<V> inner;
    final TaskGroup<V> taskGroup;

    RunnableFutureWrapper(RunnableFuture<V> inner, TaskGroup<V> taskGroup) {
      this.inner = inner;
      this.taskGroup = taskGroup;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return inner.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return inner.isCancelled();
    }

    @Override
    public boolean isDone() {
      return inner.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return inner.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return inner.get(timeout, unit);
    }

    @Override
    public void run() {
      inner.run();
    }
  }

  private class TaskThreadPool extends ThreadPoolExecutor {
    private final ConcurrentHashMap<Callable<?>, TaskGroup<?>> taskMapping = new ConcurrentHashMap<>();

    public TaskThreadPool(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public <U> Future<U> submit(Callable<U> callable, TaskGroup<U> taskGroup) {
      taskMapping.put(callable, taskGroup);
      return submit(callable);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
      System.out.println("beforeExecute: thread: " + t + ", runnable: " + r);
      super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      System.out.println("afterExecute: runnable: " + r + ", throwable: " + t);
      synchronized (TaskExecutor.this) {
        activeTasks--;
        super.afterExecute(r, t);
        if (!(r instanceof RunnableFutureWrapper)) {
          System.out.println("Not expected runnable type, got: " + r.getClass().getName());
          return;
        }
        TaskGroup<?> taskGroup = ((RunnableFutureWrapper<?>) r).taskGroup;
        boolean isQueued = taskGroup.shouldBeQueued();
        taskGroup.taskCompleted(t);

        if (taskGroup.shouldBeQueued()) {
          // if higher priority than queue top, start task with finished capacity
          if (taskGroup.shouldBeQueued() && !isQueued) {
            pendingTaskQueue.add(taskGroup);
          }
        }
        startQueuedTasks();
      }
    }

    @Override
    protected <U> RunnableFuture<U> newTaskFor(Callable<U> callable) {
      RunnableFuture<U> rf = super.newTaskFor(callable);
      RunnableFutureWrapper<U> wrappedFuture = new RunnableFutureWrapper<>(rf, (TaskGroup<U>) taskMapping.remove(callable));
      System.out.println("newTaskFor: callable: " + callable + ", runnableFuture: " + rf + ", wrapped: " + wrappedFuture);
      return wrappedFuture;
    }
  }
}
