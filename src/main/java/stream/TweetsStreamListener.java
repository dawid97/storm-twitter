package stream;

import com.twitter.clientlib.model.StreamingTweetResponse;

public interface TweetsStreamListener {
    void actionOnTweetsStream(StreamingTweetResponse streamingTweet);
}
