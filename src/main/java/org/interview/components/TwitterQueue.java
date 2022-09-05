package org.interview.components;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.interview.data.Tweet;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TwitterQueue {

    @Value(value = "${app.max_tweets}")
    private int maxTweets;

    private BlockingQueue<String> blockingQueue;

    private AtomicInteger addedTweets;

    private AtomicInteger processedTweets;

    private AtomicLong startTime;

    private AtomicReference<List<Tweet>> tweetList;

    @PostConstruct
    public void init(){
        this.blockingQueue = new LinkedBlockingDeque<>(maxTweets);
        this.addedTweets = new AtomicInteger();
        this.processedTweets = new AtomicInteger();
        this.tweetList = new AtomicReference<>();
        this.tweetList.set(new ArrayList<>());
    }

    public BlockingQueue getBlockingQueue(){
        return this.blockingQueue;
    }

    public AtomicInteger getAddedTweets() {
        return this.addedTweets;
    }

    public AtomicInteger getProcessedTweets(){
        return this.processedTweets;
    }

    public AtomicReference<List<Tweet>> getTweetList(){
        return tweetList;
    }

}
