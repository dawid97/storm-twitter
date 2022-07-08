
package bolt;

import com.twitter.clientlib.model.Tweet;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "serial"})
public class TweetFormatterBolt extends BaseRichBolt {

    private Map<String, String> tweets;

    private OutputCollector collector;

    private int tickFrequency;

    public TweetFormatterBolt(int tickFrequency) {
        this.tickFrequency = tickFrequency;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.tweets = new HashMap<>();
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tickdate", "author_id", "text"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> map = super.getComponentConfiguration();
        if (map == null) {
            map = new HashMap<>();
        }
        map.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, this.tickFrequency);
        return map;
    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void execute(Tuple input) {
        if (isTickTuple(input)) {
            for (Map.Entry<String, String> entry : tweets.entrySet()) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                Date date = new Date();
                this.collector.emit(new Values(format.format(date) + ":00", entry.getKey(), entry.getValue()));
            }
        } else {
            Tweet tweet = (Tweet) input.getValueByField("tweet");
            this.tweets.put(tweet.getAuthorId(), tweet.getText());
        }
    }
}
