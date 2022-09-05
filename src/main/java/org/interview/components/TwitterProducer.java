package org.interview.components;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.interview.oauth.twitter.TwitterAuthenticationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TwitterProducer implements Runnable{

    @Autowired
    private TwitterQueue twitterQueue;

    private AtomicReference<BufferedReader> bufferedReader = new AtomicReference<>();

    private CountDownLatch countDownLatch;


    public void init(AtomicReference<BufferedReader> bufferedReader, CountDownLatch countDownLatch) throws IOException {
        this.bufferedReader = bufferedReader;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        while(countDownLatch.getCount() != 0) {
            try {
                twitterQueue.getBlockingQueue().put(bufferedReader.get().readLine());
                log.debug("Produced tweet");
                countDownLatch.countDown();
            } catch (InterruptedException e) {
                log.info("Thread has been interrupted");
            } catch (IOException e) {
                log.error("Unable to retrieve tweet");
            }
        }
    }
}
