package com.gecko.topo.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Created by hlieu on 04/19/16.
 */
public class FileReaderSpout
        extends BaseRichSpout {

    FileReader fileReader;
    SpoutOutputCollector spoutOutputCollector;
    boolean completed = false;

    //@Override
    //public void ack() {}


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        try {
            fileReader = new FileReader(map.get("wordsfile").toString());
        }catch (FileNotFoundException fe) {
            throw new RuntimeException(fe);
        }

        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        // check if its completed
        if(completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
            return;
        }

        // read file
        BufferedReader reader = new BufferedReader(this.fileReader);
        try {
            String line = null;
            // StringBuffer sb = new StringBuffer();
            while ( (line = reader.readLine()) != null ) {
                this.spoutOutputCollector.emit(new Values(line), line);
            }
        } catch (IOException ie) {
            throw new RuntimeException(ie);
        } finally {
            /* cannot close here - because nextTuple will be called multiple times
             through the lifecycle of the topology
            */
            completed = true;
            try {
                reader.close();
            } catch (Exception e) {}

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
