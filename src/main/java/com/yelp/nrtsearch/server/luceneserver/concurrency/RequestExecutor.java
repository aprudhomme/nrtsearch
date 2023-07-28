package com.yelp.nrtsearch.server.luceneserver.concurrency;

import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class RequestExecutor<T> {
  private final TaskExecutor taskExecutor;
  private final StreamObserver<T> responseStreamObserver;

  public RequestExecutor(TaskExecutor taskExecutor, StreamObserver<T> responseStreamObserver) {
    this.taskExecutor = taskExecutor;
    this.responseStreamObserver = responseStreamObserver;
  }

  public void execute(Runnable task) {

  }

  public void executeAndThen(Runnable task, Runnable nextTask) {

  }

  public <V> void executeAndThen(Callable<V> task, Consumer<V> nextTask) {

  }

  public <V> void executeMultiAndThen(List<Runnable> tasks, Runnable nextTask) {

  }

  public <V> void executeMultiAndThen(List<Callable<V>> tasks, Consumer<List<V>> nextTask) {

  }
}
