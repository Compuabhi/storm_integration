package com.apamax;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.apama.EngineException;
import com.apama.engine.beans.EngineClientFactory;
import com.apama.engine.beans.interfaces.EngineClientInterface;
import com.apama.event.Event;
import com.apama.event.parser.DictionaryFieldType;
import com.apama.event.parser.EventType;
import com.apama.event.parser.Field;
import com.apama.event.parser.StringFieldType;
import com.apama.util.CompoundException;

public class ApamaTerminalBolt extends BaseRichBolt {

	private static final int MAX_ATTEMPT = 10;


	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	private final static EventType TUPLE_EVENT_TYPE = new EventType(
			"com.apamax.storm.Tuple",
			StringFieldType.TYPE.newField("streamId"),
			StringFieldType.TYPE.newField("sourceId"), new Field("data",
					new DictionaryFieldType<String, String>(
							StringFieldType.TYPE, StringFieldType.TYPE)));

	private EngineClientInterface engineClient = null;
	private OutputCollector _collector;

	private final String host;
	private String APAMA_HOME;
	private String APPLICATION_HOME;
	private String ANT_HOME;

	private final int port;

	public ApamaTerminalBolt(String host, int port, Properties config) {
		this.host = host;
		this.port = port;
		
		
		this.APAMA_HOME=config.getProperty("APAMA_HOME");
		this.APPLICATION_HOME=config.getProperty("APPLICATION_HOME");
		this.ANT_HOME=APAMA_HOME + "\\third_party\\jakarta-ant-1.7.1";

	}

	private void startApplication() throws CompoundException, IOException {

		ProcessBuilder pb = new ProcessBuilder("CMD", "/c", "start",
				"project.bat", "start").directory(new File(APPLICATION_HOME));
		Map<String, String> environment = pb.environment();
		environment.put("APAMA_HOME", APAMA_HOME);
		environment.put("ANT_HOME", ANT_HOME);
		pb.start();

	}

	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector collector) {

		_collector = collector;
		try {
			startApplication();

		} catch (CompoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		establishConnection();
	}

	private void establishConnection() {
		int count = 0;
		while (count++ < MAX_ATTEMPT) {
			try {
				if (engineClient == null) {
					engineClient = EngineClientFactory.createEngineClient(host,
							port, "my-tuple-process");
				}
				engineClient.connectNow();
				Thread.sleep(2000);
			} catch (CompoundException | InterruptedException e) {
			}
		}
		if (engineClient.isBeanConnected()) {
			System.out.println("################################## CONNECTED");
		} else {
			System.out
					.println("################################## NOT CONNECTED");
		}

	}

	public void execute(Tuple tuple) {

		try {
			sendEvents(tuple);
		} catch (CompoundException | InterruptedException e) {
			_collector.fail(tuple);
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
			_collector.fail(tuple);
		}
		_collector.ack(tuple);
	}

	public void sendEvents(Tuple tuple) throws CompoundException,
			InterruptedException, IOException {
		Event e = new Event(TUPLE_EVENT_TYPE);

		Map<String, String> data = new HashMap<String, String>();

		Fields fields = tuple.getFields();
		int numFields = fields.size();
		for (int i = 0; i < numFields; i++) {
			String name = fields.get(i);
			String value = (String) tuple.getValue(i);
			data.put(name, value);

		}

		e.setField("data", data);
		e.setField("streamId", tuple.getSourceStreamId());
		e.setField("sourceId", tuple.getSourceComponent());

		e.setChannel("com.apamax.storm.receive");

		try {
			engineClient.sendEvents(false, e);

		} catch (EngineException e1) {
			if (!engineClient.isBeanConnected()) {
				try {
					startApplication();
					establishConnection();
					engineClient.sendEvents(false, e);
				} catch (CompoundException e2) {
					System.out.println("Correlator is not getting started");
					_collector.fail(tuple);
				}

			}
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

		arg0.declare(new Fields());
	}
 /*
	public void cleanup() {
		ProcessBuilder pb = new ProcessBuilder("CMD",
				"/c", "start", "project.bat",
				"stop").directory(new File(APPLICATION_HOME));
		Map<String, String> environment =
				pb.environment(); environment.put("APAMA_HOME",
						APAMA_HOME); 
				environment.put("ANT_HOME",ANT_HOME);
				try { Process p = pb.start(); } 
				catch (IOException e) 

				{ 
					e.printStackTrace(); 
				}

	}   

*/
}
