import bolt.TweetFormatterBolt;
import bolt.TweetFilterBolt;
import bolt.TweetHdfsBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import spout.TwitterSpout;

public class Topology {

    static final String TOPOLOGY_NAME = "storm-twitter";

    static final String[] WORDS_FILTERS = new String[]{"drug ","drugs ", "heroin", "cocaine", "cannabis", "ketamine", "methamphetamines", "opioids", "marijuana", "crystal meth", "ecstasy"};

    static final String HDFS = "hdfs://localhost:9000";

    static final int TICK_FREQUENCY = 300; //seconds

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);

        String bearerToken = "fill with your bearer token"; // paste your bearer token

        //storm topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("TwitterSpout", new TwitterSpout(bearerToken), 1);
        topologyBuilder.setBolt("TweetFilterBolt", new TweetFilterBolt(WORDS_FILTERS), 1).shuffleGrouping("TwitterSpout");
        topologyBuilder.setBolt("TweetFormatterBolt", new TweetFormatterBolt(TICK_FREQUENCY), 1).fieldsGrouping("TweetFilterBolt", new Fields("tweet"));

        //hdfs
        Fields fields = new Fields("tickdate", "author_id", "text");
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPrefix("twitter").withExtension(".txt")
                .withPath("/storm");

        RecordFormat recordFormat = new DelimitedRecordFormat().withFields(fields);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
        SyncPolicy syncPolicy = new CountSyncPolicy(10);

        TweetHdfsBolt bolt = (TweetHdfsBolt) new TweetHdfsBolt().withFsUrl(HDFS).withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        topologyBuilder.setBolt("TweetHdfsBolt", bolt, 1).allGrouping("TweetFormatterBolt");


        try {
            final LocalCluster cluster = new LocalCluster();

            cluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    cluster.killTopology(TOPOLOGY_NAME);
                } catch (Exception e) {
                    System.out.println("something went wrong, exception: " + e.getMessage());
                }
                cluster.shutdown();
            }));
        } catch (Exception ex) {
            System.out.println("something went wrong, exception: " + ex.getMessage());
        }
    }
}
