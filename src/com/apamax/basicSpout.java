package com.apamax;








import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class basicSpout implements IRichSpout {

	SpoutOutputCollector _collector;
	Random _rand;
	private static int j=0;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
//		String[] sentences = new String[] { " 10 abhi kolkata ","15 nayan kolkata","18 monica pune","24 mack bang", " 20 abhi patna ", "30 dhoni ranchi",
//				"40 john boston", "50 rajkumar chennai", "60 raja hyd", "70 nikita delhi" };
		
		String[] sentences = new String[] { " my name is abhishek","kolkata is my home town","dhoni is great","bangalore is awesome", " honesty is best policy  ", "honesty lasts forever",
				"boston is in USA", "chennai is too hot", "hyd or telanagana choice is yours", "you are cute","hope is good thing" ," good thing never dies" };
	
		//String sentence = sentences[_rand.nextInt(sentences.length)];
		//String []strarr=sentence.trim().split("\\s+");
	if(j<12)
	{		_collector.emit(new Values(sentences[j]));
			j++;
	}	
			
	//	_collector.emit(new Values(strarr[0],strarr[1],strarr[2]));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("value","name","city"));
		declarer.declare(new Fields("sentence"));
	}

	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
