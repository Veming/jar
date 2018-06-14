package com.hrbust.su.topology;

import com.hrbust.su.bolts.*;
import com.hrbust.su.spouts.S1;
import com.hrbust.su.spouts.S2;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Main {
	public static void main(String[] args) {

		// TODO Auto-generated method stub

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("S1", new S1());
		builder.setSpout("S2", new S2());

		builder.setBolt("B11", new B11()).shuffleGrouping("S1").shuffleGrouping("S2");
		builder.setBolt("B12", new B12()).shuffleGrouping("S2");

		builder.setBolt("B21", new B21()).shuffleGrouping("B11");
		builder.setBolt("B22", new B22()).shuffleGrouping("B12");
		builder.setBolt("B23", new B23()).shuffleGrouping("B12");

		builder.setBolt("B3", new B3()).shuffleGrouping("B21").shuffleGrouping("B23").shuffleGrouping("B22");

      		Config conf = new Config();
		conf.setNumWorkers(3);
		StormSubmitter.submitTopology("topology", conf, builder.createTopology());

	}



}
