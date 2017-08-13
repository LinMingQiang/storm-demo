package com.storm.demo.hbasebolt;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt{  
	private OutputCollector outputCollector;  
	String grex=",";
	public SplitSentenceBolt(String grex){
		this.grex=grex;
	}
    @Override  
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {  
        this.outputCollector=outputCollector;  
    }  
    @Override  
    public void execute(Tuple tuple){
        // 接收到一个句子  
        String sentence = tuple.getString(0);  
        // 把句子切割为单词  
        String[] iter = sentence.split(grex);
        for(String str:iter){
        	outputCollector.emit(new Values(str)); 
        }
        outputCollector.ack(tuple);//对处理tuple进行确认。通知ack观测器。这条数据处理成功
    }  
    @Override  
    public void declareOutputFields(OutputFieldsDeclarer declarer){  
        // 定义一个字段  
        declarer.declare(new Fields("word"));  
    }  
}
