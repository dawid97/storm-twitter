package queue;

import com.twitter.clientlib.model.StreamingTweetResponse;

public interface ITweetsQueue {
    StreamingTweetResponse poll();
    void add(StreamingTweetResponse streamingTweet);
}
