package com.storm.demo.wcbolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;



public class WordCountBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5686468199447584114L;
	Map<String, Integer> counts = new HashMap<String, Integer>();
	private OutputCollector outputCollector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
	}
	@Override
	public void execute(Tuple tuple) {
        String word = tuple.getString(0);  
        Integer count = counts.get(word);  
        if(count == null)   count = 1;  
        else count++;  
        counts.put(word,count); 
        outputCollector.emit(new Values(word, count));//发送给下一个bolt
        outputCollector.ack(tuple);//通过ack观测器，这条数据处理成功
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 定义两个字段word和count
		declarer.declare(new Fields("word",  "count"));
	}
}