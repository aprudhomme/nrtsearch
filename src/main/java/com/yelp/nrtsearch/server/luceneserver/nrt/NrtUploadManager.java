package com.yelp.nrtsearch.server.luceneserver.nrt;

import com.yelp.nrtsearch.server.luceneserver.NRTPrimaryNode;
import com.yelp.nrtsearch.server.luceneserver.index.IndexDataManager;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.lucene.replicator.nrt.CopyState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NrtUploadManager implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(NrtUploadManager.class);
  private UploadTask currentTask;
  private UploadTask nextTask;
  private final UploadThread uploadThread;
  private final NRTPrimaryNode nrtPrimaryNode;
  private final IndexDataManager indexDataManager;

  private static class UploadTask implements Future<Void> {
    private CopyState copyState;
    private boolean done = false;
    private Exception exception = null;
    UploadTask(CopyState copyState) {
      this.copyState = copyState;
    }

    public CopyState updateCopyState(CopyState newState) {
      // TODO check version is greater or equal
      CopyState previous = copyState;
      copyState = newState;
      return previous;
    }

    public synchronized void setDone(Exception e) {
      done = true;
      exception = e;
      notifyAll();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      // TODO
      return false;
    }

    @Override
    public boolean isCancelled() {
      // TODO
      return false;
    }

    @Override
    public synchronized boolean isDone() {
      return done;
    }

    @Override
    public synchronized Void get() throws InterruptedException, ExecutionException {
      while (!done) {
        wait();
      }
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return null;
    }

    @Override
    public synchronized Void get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      while (!done) {
        wait(unit.toMillis(timeout));
      }
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return null;
    }
  }

  private class UploadThread extends Thread {
    private volatile boolean done = false;
    UploadThread() {
      super("TODO-thread-name");
    }

    @Override
    public void run() {
      while (!done) {
        try {
          UploadTask task;
          synchronized (NrtUploadManager.this) {
            while (!done && NrtUploadManager.this.currentTask == null) {
              NrtUploadManager.this.wait();
            }
            task = NrtUploadManager.this.currentTask;
          }
          if (done) {
            break;
          }

          try {
            // Upload data
            indexDataManager.uploadIndexData(task.copyState);

            task.setDone(null);
          } catch (Exception e) {
            logger.error("Could not complete upload task", e);
            task.setDone(e);
          }

          synchronized (NrtUploadManager.this) {
            NrtUploadManager.this.currentTask = NrtUploadManager.this.nextTask;
            NrtUploadManager.this.nextTask = null;
          }

          NrtUploadManager.this.nrtPrimaryNode.releaseCopyState(task.copyState);
        } catch (Exception e) {
          logger.error("Unexpected exception in run loop", e);
        }
      }
    }

    void close() {
      done = true;
      NrtUploadManager.this.notifyAll();
      try {
        this.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public NrtUploadManager(NRTPrimaryNode nrtPrimaryNode, IndexDataManager indexDataManager) {
    this.nrtPrimaryNode = nrtPrimaryNode;
    this.indexDataManager = indexDataManager;
    uploadThread = new UploadThread();
    uploadThread.start();
  }

  public synchronized Future<Void> uploadAsync(CopyState copyState) throws IOException {
    if (currentTask == null) {
      currentTask = new UploadTask(copyState);
      // wake up the upload thread
      this.notifyAll();
      return currentTask;
    }
    if (nextTask == null) {
      nextTask = new UploadTask(copyState);
    } else {
      CopyState previous = nextTask.updateCopyState(copyState);
      nrtPrimaryNode.releaseCopyState(previous);
    }
    return nextTask;
  }

  @Override
  public void close() throws IOException {
    uploadThread.close();
  }
}
