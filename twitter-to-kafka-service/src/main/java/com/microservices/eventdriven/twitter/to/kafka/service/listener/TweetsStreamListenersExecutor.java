package com.microservices.eventdriven.twitter.to.kafka.service.listener;

import com.twitter.clientlib.ApiException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TweetsStreamListenersExecutor {

  private final static int TIMEOUT_MILLIS = 60000;
  private final static int SLEEP_MILLIS = 100;
  private final static int BACKOFF_SLEEP_INTERVAL_MILLIS = 5000;
  private TweetsQueuer tweetsQueuer;
  private final ITweetsQueue tweetsQueue = new LinkedListTweetsQueue();
  private final AtomicBoolean isRunning = new AtomicBoolean(true);
  private final AtomicLong tweetStreamedTime = new AtomicLong(0);
  private StreamingHandler<?> streamingHandler;
  private long reconnecting = 0;

  public StreamListenersExecutorBuilder stream() {
    return new StreamListenersExecutorBuilder();
  }

  private void shutdown(Exception e) {
    e.printStackTrace();
    shutdown();
  }

  public void shutdown() {
    isRunning.set(false);
    log.info("TweetsStreamListenersExecutor is shutting down.");
  }

  private InputStream connectStream() throws ApiException {
    return streamingHandler.connectStream();
  }

  private void resetTweetStreamedTime() {
    tweetStreamedTime.set(System.currentTimeMillis());
  }

  private boolean isTweetStreamedError() {
    return System.currentTimeMillis() - tweetStreamedTime.get() > TIMEOUT_MILLIS;
  }

  private void restartTweetsQueuer() {
    tweetsQueuer.shutdownQueuer();
    if (reconnecting < 7) {
      reconnecting++;
    }
    try {
      log.info("sleeping " + BACKOFF_SLEEP_INTERVAL_MILLIS * reconnecting);
      Thread.sleep(BACKOFF_SLEEP_INTERVAL_MILLIS
          * reconnecting); // Wait a bit before starting the TweetsQueuer and calling the API again.
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    tweetsQueuer.interrupt();
    tweetsQueuer = new TweetsQueuer();
    tweetsQueuer.start();
  }

  private class TweetsListenersExecutor extends Thread {

    @Override
    public void run() {
      processTweets();
    }

    private void processTweets() {
      String tweetString;
      try {
        while (isRunning.get()) {
          tweetString = tweetsQueue.poll();
          if (tweetString == null) {
            Thread.sleep(SLEEP_MILLIS);
            continue;
          }
          try {
            if (!streamingHandler.processAndVerifyStreamingObject(tweetString)) {
              restartTweetsQueuer();
            }
          } catch (Exception interExcept) {
            interExcept.printStackTrace();
          }
        }
      } catch (Exception e) {
        shutdown(e);
      }
    }
  }

  private class TweetsQueuer extends Thread {

    private boolean isReconnecting;

    @Override
    public void run() {
      isReconnecting = false;
      queueTweets();
    }

    public void shutdownQueuer() {
      isReconnecting = true;
    }

    private void queueTweets() {
      String line;
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(connectStream()))) {
        while (isRunning.get() && !isReconnecting) {
          line = reader.readLine();
          resetTweetStreamedTime();
          if (line == null || line.isEmpty()) {
            Thread.sleep(SLEEP_MILLIS);
            continue;
          }
          tweetsQueue.add(line);
        }
      } catch (InterruptedIOException e) {
        e.printStackTrace();
      } catch (Exception e) {
        shutdown(e);
      }
    }
  }

  private class StreamTimeoutChecker extends Thread {

    @Override
    public void run() {
      checkTimes();
    }

    public void checkTimes() {
      resetTweetStreamedTime();
      while (isRunning.get()) {
        if (isTweetStreamedError()) {
          shutdown(new ApiException("Tweets are not streaming"));
        }
        try {
          Thread.sleep(SLEEP_MILLIS);
        } catch (InterruptedException interExcept) {
          interExcept.printStackTrace();
        }
      }
    }
  }

  public class StreamListenersExecutorBuilder {

    public StreamListenersExecutorBuilder streamingHandler(StreamingHandler<?> streamHandler) {
      streamingHandler = streamHandler;
      return this;
    }

    public void executeListeners() throws ApiException {
      if (streamingHandler == null) {
        throw new ApiException("Please set a streamingHandler");
      }

      TweetsListenersExecutor tweetsListenersExecutor;
      StreamTimeoutChecker timeoutChecker;
      tweetsQueuer = new TweetsQueuer();
      tweetsListenersExecutor = new TweetsListenersExecutor();
      timeoutChecker = new StreamTimeoutChecker();
      tweetsListenersExecutor.start();
      tweetsQueuer.start();
      timeoutChecker.start();
    }
  }
}

interface ITweetsQueue {

  String poll();

  void add(String streamingTweet);
}

class LinkedListTweetsQueue implements ITweetsQueue {

  private final Queue<String> tweetsQueue = new LinkedList<>();

  @Override
  public String poll() {
    return tweetsQueue.poll();
  }

  @Override
  public void add(String streamingTweet) {
    tweetsQueue.add(streamingTweet);
  }
}