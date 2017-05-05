



package com.apamax;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

class largestartingSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	
//	private static ConcurrentLinkedQueue<MSG> msgQ;
//	private static ConcurrentHashMap<Integer, MSG> pendingMap = new ConcurrentHashMap<Integer, MSG>();
	private static Queue<MSG> msgQ;
	private static HashMap<Integer, MSG> pendingMap = new HashMap<Integer, MSG>();
	 private static int flag=0;

	static class MSG {

		MSG(Object sentences, int id) {
			this.msgID = id;
			this.msg = sentences;

		}

		int msgID;
		Object msg;
	}

	private static void addElements() {
		int i = 0;
		int count=0;
		msgQ = new LinkedList<MSG>();

		String[] sentences = new String[] { " my name is abhishek","kolkata is my home town","federer is great","LA is awesome", " honesty is best policy ", "honesty lasts forever",
				"boston is in USA", "chennai is too hot", "chicken or fish, choice is yours", "STORM is a realtime processing system ","hope is good thing" ," good thing never dies" };
	
	if(flag==1){

		while (i < 200) {
			MSG m1 = new MSG((sentences[(i%12)]), count++);
			msgQ.add(m1);
			System.out.println(m1);
			i++;
			}
		}	
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		flag++;
		addElements();

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		if (!msgQ.isEmpty()) {
			MSG m1 = (MSG) msgQ.poll();
			pendingMap.put(m1.msgID, m1);
			_collector.emit(new Values(m1.msg), m1.msgID);

		}

	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object msgId) {

		msgQ.add(pendingMap.get(msgId));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("sentence"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
