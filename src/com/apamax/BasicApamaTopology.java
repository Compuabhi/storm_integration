package com.apamax;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class BasicApamaTopology {

	public static void main(String[] args) throws Exception {
		System.setProperty("APAMA_LOG_IMPL", "simple");
		TopologyBuilder builder = new TopologyBuilder();

		Properties config = new Properties();
		
		try {
			  config.load(new FileInputStream("C:\\work\\ws\\apama\\ApamaCluster\\src\\com\\apamax\\config.properties"));
			} catch (IOException e) {
				
			}
		
		
		builder.setSpout("Temp_Spout", new startingSpout(), 1);
//		builder.setSpout("Temp_Spout", new basicSpout(), 2);
		builder.setBolt("Basic_Bolt", new SplitBolt(), 2).shuffleGrouping("Temp_Spout");
		builder.setBolt("Apama_Bolt###1", new ApamaTerminalBolt("localhost",15903,config), 3).fieldsGrouping("Basic_Bolt",new Fields("word"));
		
		// create the default config object
		Config conf = new Config();

		// set the config in debugging mode
		conf.setDebug(true);

		//conf.TOPOLOGY_MESSAGE_TIMEOUT_SECS.equals(".001");

		if (args != null && args.length > 0) {

			// run it in a live cluster

			// set the number of workers for running all spout and bolt tasks
			conf.setNumWorkers(3);
		

			// create the topology and submit with config
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());

		} else {

			// run it in a simulated local cluster

			// create the local cluster instance
			LocalCluster cluster = new LocalCluster();
		
//			conf.setNumWorkers(3);
			// submit the topology to the local cluster
			cluster.submitTopology("Apama_Topology", conf, builder.createTopology());

			// let the topology run for 20 seconds. note topologies never
			// terminate!
			Thread.sleep(20000);

			// kill the topology
			cluster.killTopology("Apama_Topology");

			// we are done, so shutdown the local cluster
			cluster.shutdown();
		}

	}

}