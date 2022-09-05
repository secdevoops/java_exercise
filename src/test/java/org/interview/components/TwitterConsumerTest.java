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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

@Slf4j
@ExtendWith(MockitoExtension.class)
@TestInstance(PER_CLASS)
public class TwitterConsumerTest {

    private static final String TWEET_1 = "{\"created_at\":\"Wed Jul 27 07:25:26 +0000 2022\",\"id\":1552193429320257536,"
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

    private static final String TWEET_2 = "{\"created_at\":\"Wed Jul 27 07:25:30 +0000 2022\",\"id\":1552193445162151936,"
            + "\"id_str\":\"1552193445162151936\",\"text\":\"Somebody to love ~ Justin Bieber, Usher #Vh1Playlist\","
            + "\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/android\\\" rel=\\\"nofollow\\\"\\u003eTwitter for Android\\u003c\\/a\\u003e\","
            + "\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,"
            + "\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":1403222168364740615,"
            + "\"id_str\":\"1403222168364740615\",\"name\":\"Sunaina Mahure\",\"screen_name\":\"1023sunaina\",\"location\":null,\"url\":null,"
            + "\"description\":\"I wanted to wrap you in\\nMy warm embrace\\nAnd make it last forever \\u2764\\ufe0flove you breezy\","
            + "\"translator_type\":\"none\",\"protected\":false,\"verified\":false,\"followers_count\":27,\"friends_count\":27,\"listed_count\":0,"
            + "\"favourites_count\":420,\"statuses_count\":38057,\"created_at\":\"Fri Jun 11 05:27:03 +0000 2021\",\"utc_offset\":null,"
            + "\"time_zone\":null,\"geo_enabled\":false,\"lang\":null,\"contributors_enabled\":false,\"is_translator\":false,"
            + "\"profile_background_color\":\"F5F8FA\",\"profile_background_image_url\":\"\",\"profile_background_image_url_https\":\"\","
            + "\"profile_background_tile\":false,\"profile_link_color\":\"1DA1F2\",\"profile_sidebar_border_color\":\"C0DEED\","
            + "\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,"
            + "\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/1547973259718582272\\/qts_s2x7_normal.jpg\","
            + "\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/1547973259718582272\\/qts_s2x7_normal.jpg\","
            + "\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/1403222168364740615\\/1658901726\",\"default_profile\":true,"
            + "\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null,\"withheld_in_countries\":[]},"
            + "\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"is_quote_status\":false,\"quote_count\":0,\"reply_count\":0,"
            + "\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[{\"text\":\"Vh1Playlist\",\"indices\":[40,52]}],\"urls\":[],"
            + "\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"filter_level\":\"low\",\"lang\":\"en\","
            + "\"timestamp_ms\":\"1658906730343\"}";

    private TwitterQueue twitterQueue;


    @Test
    public void testReachMaxTweets() throws InterruptedException {
        twitterQueue = new TwitterQueue();
        ReflectionTestUtils.setField(twitterQueue, "maxTweets", 2);
        twitterQueue.init();
        twitterQueue.getBlockingQueue().put(TWEET_1);
        twitterQueue.getBlockingQueue().put(TWEET_2);

        TwitterConsumer twitterConsumer = spy(new TwitterConsumer());
        ReflectionTestUtils.setField(twitterConsumer, "maxTweets", 2);
        ReflectionTestUtils.setField(twitterConsumer, "maxTimeInMilliseconds", 2000);
        ReflectionTestUtils.setField(twitterConsumer, "twitterQueue", twitterQueue);
        Thread consumerThread = new Thread(twitterConsumer);
        consumerThread.start();
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            log.error("Error on thread", e);
        }
        assertEquals(0,twitterQueue.getBlockingQueue().size());
        assertEquals(2, twitterQueue.getTweetList().get().size());
        assertEquals("1552193429320257536", twitterQueue.getTweetList().get().get(0).getId());
        assertEquals("1552193445162151936", twitterQueue.getTweetList().get().get(1).getId());
    }

    @Test
    public void testReachMaxTime(){
        twitterQueue = new TwitterQueue();
        ReflectionTestUtils.setField(twitterQueue, "maxTweets", 2);
        twitterQueue.init();

        TwitterConsumer twitterConsumer = spy(new TwitterConsumer());
        ReflectionTestUtils.setField(twitterConsumer, "maxTweets", 2);
        ReflectionTestUtils.setField(twitterConsumer, "maxTimeInMilliseconds", 2);
        ReflectionTestUtils.setField(twitterConsumer, "twitterQueue", twitterQueue);
        Thread consumerThread = new Thread(twitterConsumer);
        consumerThread.start();
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            log.error("Error on thread", e);
        }
        assertEquals(0,twitterQueue.getBlockingQueue().size());
        assertEquals(0, twitterQueue.getTweetList().get().size());
    }

}
