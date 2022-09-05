package org.interview.components;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.interview.oauth.twitter.TwitterAuthenticationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

@Slf4j
@ExtendWith(MockitoExtension.class)
@TestInstance(PER_CLASS)
public class TwitterProducerTest {

    private static final String TWEET = "{\"created_at\":\"Wed Jul 27 07:25:26 +0000 2022\",\"id\":1552193429320257536,"
            + "\"id_str\":\"1552193429320257536\",\"text\":\"#Vh1Playlist Play As Long As You Love Me by Justin Bieber!!\","
            + "\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/android\\\" rel=\\\"nofollow\\\"\\u003eTwitter for Android\\u003c\\/a\\u003e\","
            + "\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,"
            + "\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":1366457232636846080,"
            + "\"id_str\":\"1366457232636846080\",\"name\":\"Sanilla\",\"screen_name\":\"jimintequiero_\",\"location\":\"with jimin\",\"url\":null,"
            + "\"description\":null,\"translator_type\":\"none\",\"protected\":false,\"verified\":false,\"followers_count\":228,\"friends_count\":199,"
            + "\"listed_count\":1,\"favourites_count\":1363,\"statuses_count\":16021,\"created_at\":\"Mon Mar 01 18:36:19 +0000 2021\","
            + "\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":true,\"lang\":null,\"contributors_enabled\":false,\"is_translator\":false,"
            + "\"profile_background_color\":\"F5F8FA\",\"profile_background_image_url\":\"\",\"profile_background_image_url_https\":\"\","
            + "\"profile_background_tile\":false,\"profile_link_color\":\"1DA1F2\",\"profile_sidebar_border_color\":\"C0DEED\","
            + "\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,"
            + "\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/1552184955203031040\\/uMQiRENc_normal.jpg\","
            + "\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/1552184955203031040\\/uMQiRENc_normal.jpg\","
            + "\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/1366457232636846080\\/1656256212\",\"default_profile\":true,"
            + "\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null,\"withheld_in_countries\":[]},"
            + "\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"is_quote_status\":false,\"quote_count\":0,\"reply_count\":0,"
            + "\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[{\"text\":\"Vh1Playlist\",\"indices\":[0,12]}],\"urls\":[],"
            + "\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"filter_level\":\"low\",\"lang\":\"en\","
            + "\"timestamp_ms\":\"1658906726566\"}";

    @Mock
    private HttpRequestFactory httpRequestFactory;

    @Mock
    private HttpRequest request;

    @Mock
    private HttpResponse response;

    @Mock
    private InputStream inputStream;

    private TwitterQueue twitterQueue;

    private MockedConstruction<BufferedReader> mockedConstruction;

    @BeforeAll
    void beforeAll(){
        mockedConstruction = Mockito.mockConstruction(BufferedReader.class,
                (bufferedReader, context) -> {
                    when(bufferedReader.readLine()).thenReturn(TWEET);
                });

    }

    @AfterAll
    public void afterEach() {
        mockedConstruction.close();
    }

    @BeforeEach
    void beforeEach() throws IOException {
        when(httpRequestFactory.buildGetRequest(any())).thenReturn(request);
        when(request.execute()).thenReturn(response);
        when(response.getContent()).thenReturn(inputStream);
        twitterQueue = new TwitterQueue();
        ReflectionTestUtils.setField(twitterQueue, "maxTweets", 2);
        twitterQueue.init();
    }

        @Test
    public void testReachMaxTweets() throws TwitterAuthenticationException, IOException, InterruptedException {
        TwitterProducer twitterProducer = spy(new TwitterProducer());
        ReflectionTestUtils.setField(twitterProducer, "search", "bieber");
        ReflectionTestUtils.setField(twitterProducer, "maxTweets", 2);
        ReflectionTestUtils.setField(twitterProducer, "maxTimeInMilliseconds", 2000);
        ReflectionTestUtils.setField(twitterProducer, "twitterQueue", twitterQueue);
        //twitterProducer.init(httpRequestFactory);
        Thread producerThread = new Thread(twitterProducer);
        producerThread.start();
        try {
            producerThread.join();
        } catch (InterruptedException e) {
            log.error("Error on thread", e);
        }
        assertEquals(2,twitterQueue.getBlockingQueue().size());
        assertEquals(TWEET, twitterQueue.getBlockingQueue().take());
        assertEquals(1,twitterQueue.getBlockingQueue().size());
        assertEquals(TWEET, twitterQueue.getBlockingQueue().take());
        assertEquals(0,twitterQueue.getBlockingQueue().size());
    }

    @Test
    public void testReachMaxTime() throws TwitterAuthenticationException, IOException {
        TwitterProducer twitterProducer = spy(new TwitterProducer());
        ReflectionTestUtils.setField(twitterProducer, "search", "martes");
        ReflectionTestUtils.setField(twitterProducer, "maxTweets", 2);
        ReflectionTestUtils.setField(twitterProducer, "maxTimeInMilliseconds", 1);
        ReflectionTestUtils.setField(twitterProducer, "twitterQueue", twitterQueue);
        //twitterProducer.init(httpRequestFactory);
        Thread producerThread = new Thread(twitterProducer);
        producerThread.start();
        try {
            producerThread.join();
        } catch (InterruptedException e) {
            log.error("Error on thread", e);
        }
        assertEquals(1,twitterQueue.getBlockingQueue().size());
    }

}
