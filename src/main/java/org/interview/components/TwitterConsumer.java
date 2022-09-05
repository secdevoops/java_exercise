package org.interview.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.interview.data.Tweet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TwitterConsumer implements Runnable{

    @Value(value = "${app.max_tweets}")
    private int maxTweets;

    @Value(value = "${app.max_time_in_milliseconds}")
    private int maxTimeInMilliseconds;

    @Autowired
    private TwitterQueue twitterQueue;

    private CountDownLatch countDownLatch;

    public void init(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        while(countDownLatch.getCount() != 0) {
            try {
                log.debug("Running consumer thread");
                ObjectMapper mapper = new ObjectMapper();
                String jsonTweet = (String) twitterQueue.getBlockingQueue().poll(maxTimeInMilliseconds, TimeUnit.MILLISECONDS);
                log.debug("Consumed tweet");
                if (jsonTweet != null) {
                    try {
                        log.debug("Got tweet from queue");
                        Tweet tweet = mapper.readValue(jsonTweet, Tweet.class);
                        synchronized (this) {
                            if(twitterQueue.getTweetList().get().size()<maxTweets) {
                                twitterQueue.getTweetList().get().add(tweet);
                            }
                        }
                        countDownLatch.countDown();
                    } catch (JsonProcessingException e) {
                        log.error("Error while mapping tweet", e);
                    }
                }
                log.debug("Finished consumer thread");
            } catch (InterruptedException e) {
                log.info("Thread has been interrupted");
            }
        }
    }
}
