
package bolt;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

@SuppressWarnings("serial")
public class TweetHdfsBolt extends org.apache.storm.hdfs.bolt.HdfsBolt {

    @Override
    @SuppressWarnings("rawtypes")
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        this.hdfsConfig.addResource(new Path("/hadoop-3.3.0/etc/hadoop/hdfs-site.xml"));
        this.hdfsConfig.addResource(new Path("/hadoop-3.3.0/etc/hadoop/core-site.xml"));
        this.fs = FileSystem.get(URI.create(this.fsUrl), this.hdfsConfig);
    }
}
