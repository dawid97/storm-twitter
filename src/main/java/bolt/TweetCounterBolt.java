
package bolt;

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
public class TweetCounterBolt extends BaseRichBolt {

    private Map<String, Long> counter;

    private OutputCollector collector;

    private String[] wordsFilters;

    private int tickFrequency;

    public TweetCounterBolt(String[] wordsFilters, int tickFrequency) {
        this.wordsFilters = wordsFilters;
        this.tickFrequency = tickFrequency;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.counter = new HashMap<>();
        for (String filter : this.wordsFilters) {
            this.counter.put(filter, 0L);
        }
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("filter", "tickdate", "totalcount"));
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
            for (String filter : this.wordsFilters) {
                System.out.println("TICK with [" + filter + "]");
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                Date date = new Date();
                this.collector.emit(new Values(filter, format.format(date) + ":00", this.counter.get(filter)));
                // System.out.println("Number of tweet with [" + filter + "] at
                // [" + format.format(date) + "] : " + counter.get(filter));
                this.counter.put(filter, 0L);
            }
        } else {
            String filter = (String) input.getValueByField("filter");
            Long count = this.counter.get(filter);
            count = count == null ? 1L : count + 1;
            this.counter.put(filter, count);
        }
        System.out.println("Counter: " + this.counter.toString());
    }
}
