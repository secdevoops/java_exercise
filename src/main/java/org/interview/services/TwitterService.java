package org.interview.services;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.interview.components.TwitterConsumer;
import org.interview.components.TwitterProducer;
import org.interview.components.TwitterQueue;
import org.interview.data.Tweet;
import org.interview.oauth.twitter.TwitterAuthenticationException;
import org.interview.oauth.twitter.TwitterAuthenticator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TwitterService {

    public static final String ENDPOINT_STREAM_TWITTER = "https://stream.twitter.com/1.1/statuses/filter.json?track=";

    @Value(value = "${twitter.search}")
    private String search;

    @Value(value = "${app.max_tweets}")
    private int maxTweets;

    @Value(value = "${app.max_time_in_milliseconds}")
    private int maxTimeInMilliseconds;

    @Value(value = "${twitter.consumer_key}")
    private String consumerKey;

    @Value(value = "${twitter.consumer_secret}")
    private String consumerSecret;

    @Value(value = "${app.number_of_threads}")
    private int numberOfThreads;


    @Autowired
    private TwitterProducer twitterProducer;

    @Autowired
    private TwitterConsumer twitterConsumer;

    @Autowired
    private TwitterQueue twitterQueue;

    public void findTweets() throws IOException, TwitterAuthenticationException, InterruptedException {

        ObjectMapper mapper = new ObjectMapper();

        AtomicReference<BufferedReader> bufferedReader = twitterAuthentication();

        CountDownLatch producerCountDownLatch = new CountDownLatch(maxTweets);
        CountDownLatch consumerCountDownLatch = new CountDownLatch(maxTweets);

        twitterProducer.init(bufferedReader, producerCountDownLatch);
        twitterConsumer.init(consumerCountDownLatch);

        log.info("Start getting tweets");

        long startTime = System.currentTimeMillis();

        ExecutorService executorProducerService = Executors.newFixedThreadPool(numberOfThreads);
        ExecutorService executorConsumerService = Executors.newFixedThreadPool(numberOfThreads);

        IntStream.range(0, numberOfThreads).forEach( i -> {
            executorProducerService.execute(new Thread(twitterProducer));
            executorConsumerService.execute(new Thread(twitterConsumer));
        });

        producerCountDownLatch.await(maxTimeInMilliseconds, TimeUnit.MILLISECONDS);
        long endTime = System.currentTimeMillis();
        executorProducerService.shutdown();

        consumerCountDownLatch.await(maxTimeInMilliseconds, TimeUnit.MILLISECONDS);
        executorConsumerService.shutdown();


        List<Tweet> tweets = twitterQueue.getTweetList().get();
        log.info("Showing tweets sorted");
        tweets.stream()
                .sorted()
                .forEach(tweet -> {
                    try {
                        log.info(mapper.writeValueAsString(tweet));
                    } catch (JsonProcessingException e) {
                        log.error("Error while mapping tweet");
                    }
                });

        log.info("Number of Tweets retrieved {} in {} seconds.", tweets.size(), (endTime-startTime)/1000);
    }

    private AtomicReference<BufferedReader> twitterAuthentication() throws TwitterAuthenticationException, IOException {
        TwitterAuthenticator twitterAuthenticator = new TwitterAuthenticator(System.out, consumerKey, consumerSecret);
        HttpRequestFactory httpRequestFactory = twitterAuthenticator.getAuthorizedHttpRequestFactory();
        HttpRequest request = httpRequestFactory.buildGetRequest(new GenericUrl(ENDPOINT_STREAM_TWITTER.concat(search)));
        HttpResponse response = request.execute();
        InputStream inputStream = response.getContent();
        AtomicReference<BufferedReader> bufferedReader = new AtomicReference<>();
        bufferedReader.set(new BufferedReader(new InputStreamReader(inputStream)));
        return bufferedReader;
    }

}
