
package spout;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import stream.TweetsStreamListener;
import stream.TweetsStreamListenersExecutor;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings({"rawtypes", "serial"})
public class TwitterSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private LinkedBlockingQueue<Tweet> tweetQueue;

    private String bearerToken;

    private InputStream streamResult;

    public TwitterSpout(String bearerToken) {
        this.bearerToken = bearerToken;
    }

    private class Responder implements TweetsStreamListener {

        @Override
        public void actionOnTweetsStream(StreamingTweetResponse streamingTweet) {
            if(streamingTweet == null) {
                System.err.println("Error: actionOnTweetsStream - streamingTweet is null ");
                return;
            }

            if(streamingTweet.getErrors() != null) {
                streamingTweet.getErrors().forEach(System.out::println);
            } else if (streamingTweet.getData() != null) {
                System.out.println("New streaming tweet: " + streamingTweet.getData().getText());
                TwitterSpout.this.tweetQueue.offer(streamingTweet.getData());
            }
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.tweetQueue = new LinkedBlockingQueue<>(1000);
        this.collector = collector;

        TwitterCredentialsBearer twitterCredentialsBearer = new TwitterCredentialsBearer(bearerToken);

        TwitterApi apiInstance = new TwitterApi(twitterCredentialsBearer);

        Set<String> tweetFields = new HashSet<>();
        tweetFields.add("author_id");
        tweetFields.add("id");
        tweetFields.add("created_at");

        try {
            streamResult = apiInstance.tweets().sampleStream()
                    .backfillMinutes(0)
                    .tweetFields(tweetFields)
                    .execute();

            Responder responder = new Responder();
            TweetsStreamListenersExecutor tsle = new TweetsStreamListenersExecutor(streamResult);
            tsle.addListener(responder);
            tsle.executeListeners();
        } catch (ApiException e) {
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        Tweet ret = this.tweetQueue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            this.collector.emit(new Values(ret));
        }
    }

    @Override
    public void close() {
        try {
            streamResult.close();
        } catch (IOException ex) {
            System.out.println("something went wrong, exception: " + ex.getMessage());
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
}
