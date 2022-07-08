
package bolt;

import com.twitter.clientlib.model.Tweet;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

@SuppressWarnings({"rawtypes", "serial"})
public class TweetFilterBolt extends BaseRichBolt {

    private OutputCollector collector;

    private final String[] wordsFilters;

    public TweetFilterBolt(String[] wordsFilters) {
        this.wordsFilters = wordsFilters;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Tweet tweet = (Tweet) input.getValueByField("tweet");
        String text = tweet.getText();
        for (String filter : this.wordsFilters) {
            if (text.toLowerCase().contains(filter)) {
                int length = text.length() - 1;
                if (length > 100) {
                    length = 100;
                }
                System.out.println("Received tweet with [" + filter + "] : " + text.substring(0, length));
                this.collector.emit(new Values(tweet));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
}
