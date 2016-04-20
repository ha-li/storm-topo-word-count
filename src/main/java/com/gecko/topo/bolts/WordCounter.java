package com.gecko.topo.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hlieu on 04/3/16.
 */
public class WordCounter extends BaseBasicBolt {
    Integer id;
    String name;
    Map<String, Integer> counters;

    public void cleanup() {
        System.out.println("-- Word Counter [" + name + "-" + id + "] --");
        for(Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    public void prepare(Map stormConf, TopologyContext context) {
        this.counters = new HashMap<String, Integer>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = input.getString(0);

        if(!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            // increment the current value
            counters.put(str, counters.get(str) + 1);
        }
    }
}
