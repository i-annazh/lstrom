package com.caiex.lstrom.spout;

import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


/**
 * spout:数据流生成者
 * @author DELL
 *
 */
public class SentenceSpout extends BaseRichSpout{
	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	
	private String[] sentences = {
			"my dog has fleas",
			"i like cold beverages",
			"the dog ate my homework",
			"i don't think i like fleas"
	};
	
	private int index = 0 ;

	//nextTuple() 方法是所有spout 实现的核心所在，Storm 通过调用这个方法向输出的 collector 发射tuple
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index]));
		index++;
		if(index >= sentences.length) {
			index = 0;
		}
		
	}

	//Open() 方法在ISpout 接口中定义，所有Spout 组件在初始化时调用这个方法。Open() 方法接收三个参数，一个包含了Storm 配置信息的map，
	//TopologyContext 对象提供了 topology 中组件的信息，SpoutOutputCollector 对象提供了发射tuple 的方法
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		
		this.collector = collector;
	}

	//Storm 的组件通过这个方法告诉Storm 该组件会发射哪些数据流， 每个数据流的 tuple 中包含哪些字段。
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("sentences"));
	}
	
}
