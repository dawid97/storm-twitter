package queue;

import com.twitter.clientlib.model.StreamingTweetResponse;

import java.util.LinkedList;
import java.util.Queue;

public class LinkedListTweetsQueue implements ITweetsQueue {

    private final Queue<StreamingTweetResponse> tweetsQueue = new LinkedList<>();

    @Override
    public StreamingTweetResponse poll() {
        return tweetsQueue.poll();
    }

    @Override
    public void add(StreamingTweetResponse streamingTweet) {
        tweetsQueue.add(streamingTweet);
    }
}
