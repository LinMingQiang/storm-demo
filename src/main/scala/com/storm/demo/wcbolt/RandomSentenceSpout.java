package com.storm.demo.wcbolt;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout { 
		/**
	 * 
	 */
	private static final long serialVersionUID = 8430031752411843747L;
		SpoutOutputCollector _collector;  
        Random _rand;  
        @Override  
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){  
            _collector = collector;  
            _rand = new Random();  
        }  
          
        @Override  
        public void nextTuple(){ 
            // 睡眠一段时间后再产生一个数据  
            Utils.sleep(100);  
            // 句子数组  
            String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",  
                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };  
            // 随机选择一个句子  
            String sentence = sentences[_rand.nextInt(sentences.length)];  
            // 发射该句子给Bolt  
            _collector.emit(new Values(sentence));  
        }  
          
        // 确认函数  
        @Override  
        public void ack(Object id){ 
        	System.out.println("## RandomSentenceSpout ACK ##");
        }  
          
        // 处理失败的时候调用  
        @Override  
        public void fail(Object id){  
        	System.out.println("## RandomSentenceSpout Failed ##");
        }  
          
        @Override  
        public void declareOutputFields(OutputFieldsDeclarer declarer){  
            // 定义一个字段word  
            declarer.declare(new Fields("str"));  
        }  
    }  