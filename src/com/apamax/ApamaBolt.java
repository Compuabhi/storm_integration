package com.apamax;


import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.apama.engine.beans.EngineClientFactory;
import com.apama.engine.beans.interfaces.ConsumerOperationsInterface;
import com.apama.engine.beans.interfaces.EngineClientInterface;
import com.apama.event.Event;
import com.apama.event.parser.DictionaryFieldType;
import com.apama.event.parser.EventParser;
import com.apama.event.parser.EventType;
import com.apama.event.parser.Field;
import com.apama.event.parser.StringFieldType;
import com.apama.util.CompoundException;

public class ApamaBolt extends BaseRichBolt  {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	private static EventType TUPLE_EVENT_TYPE = new EventType(
			"com.apamax.storm.Tuple",
			StringFieldType.TYPE.newField("streamId"),
			StringFieldType.TYPE.newField("sourceId"), new Field("data",
					new DictionaryFieldType<String, String>(
							StringFieldType.TYPE, StringFieldType.TYPE)));
	

	private static EventParser parser = new EventParser(TUPLE_EVENT_TYPE);

	private static EngineClientInterface engineClient = null;

	private static OutputCollector _collector;

	private static ConsumerOperationsInterface myConsumer;

	private Map map;
	private TopologyContext topologyContext;
	private final String host;
	private final int port;

	/*
	 * public ApamaBolt() { this("localhost", 15903); }
	 */
	public ApamaBolt(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector collector) {
		_collector = collector;
		System.out.println("beginning of prepare method");

		this.topologyContext = topologyContext;
		this.map = map;

		try {
			System.out.println("before initialize method calling");
			initialize();
		} catch (CompoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	

	}

	private void initialize() throws CompoundException {
		System.out.println("beginning of initialize method");

		engineClient = EngineClientFactory.createEngineClient(host, port,
				"my-tuple-process");
		engineClient.connectNow();

	}

	public void execute(Tuple tuple) {
		System.out.println("beginning of execute method");
		
		try {
			sendEvents(tuple);
		} catch (CompoundException | InterruptedException e) {
			e.printStackTrace();
		}
		 
		_collector.ack(tuple);
	}

	public void sendEvents(Tuple tuple) throws CompoundException,
			InterruptedException {
		System.out.println("beginning of sendEvents method");
		Event e = new Event(TUPLE_EVENT_TYPE);

		/*
		 * EngineStatus status = engineClient.getRemoteStatus();
		 * System.out.println(); System.out.println(status);
		 */

		Map<String, String> data = new HashMap<String, String>();

		Fields fields = tuple.getFields();
		int numFields = fields.size();
		for (int i = 0; i < numFields; i++) {
			String name = fields.get(i);
			//tupleFieldNames.add(name);
			String value = (String) tuple.getValue(i);
			data.put(name, value);
			
		}
		
		e.setField("data", data);
		e.setField("streamId", tuple.getSourceStreamId());
		e.setField("sourceId", tuple.getSourceComponent());
		e.setChannel("com.apamax.storm.receive");
		engineClient.sendEvents(false, e);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {

		arg0.declare(new Fields());
	}

}