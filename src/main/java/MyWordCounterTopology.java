import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.gecko.topo.bolts.WordCounter;
import com.gecko.topo.spout.FileReaderSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hlieu on 04/19/16.
 */
public class MyWordCounterTopology {
    private static final int SPOUT_PARALLEL = 1;
    public static void main(String[] args) throws Exception {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("word-reader", new FileReaderSpout());
        topologyBuilder.setBolt("word-counter", new WordCounter()).shuffleGrouping("word-reader");

        Config config = new Config();
        config.put("wordsfile", args[0]);
        config.setDebug(true);
        config.setMaxSpoutPending(SPOUT_PARALLEL);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("My-Word-Counter-Topology", config, topologyBuilder.createTopology());
        Thread.sleep(2000);
        cluster.shutdown();
    }
}
