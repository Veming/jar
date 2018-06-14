package com.hrbust.su.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class S2 extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private TopologyContext context;

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
    }

    @Override
    public void nextTuple() {
        System.out.println("S2");
        collector.emit(new Values("S2"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("S"));
    }
}
