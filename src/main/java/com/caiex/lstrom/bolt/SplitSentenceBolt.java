package com.caiex.lstrom.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * bolt运算:实现语句分割bolt
 * @author DELL
 *
 */
public class SplitSentenceBolt extends BaseRichBolt{
	
	private OutputCollector collector;

	//每当从订阅的数据流中接收一个 tuple，都会调用这个方法
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentences");
		String[] words = sentence.split(" ");
		for(String word:words) {
			this.collector.emit(new Values(word));
		}
		
	}

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		
	}
	
}
