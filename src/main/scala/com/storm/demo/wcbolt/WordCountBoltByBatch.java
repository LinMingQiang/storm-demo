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



public class WordCountBoltByBatch extends BaseRichBolt {
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
	//定时器.如果加了这个函数，那在execute必须加TupleUtils.isTick(tuple)的判断。不然tuple里面会带一个Long类型的数据。定时会
	//发一条Long类型的数据来做判断。可以打印出来看一下就知道了
	 @Override  
	    public Map<String, Object> getComponentConfiguration() {  
	        Map<String, Object> conf = new HashMap<String, Object>();  
	        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10L);//加入Tick时间窗口，进行统计  。1s一次。如果不要窗口就去掉
	        return conf;  
	    }
	@Override
	public void execute(Tuple tuple) {
	if(TupleUtils.isTick(tuple)){  
			//一般写数据库 等操作都是 隔段时间做一次。 
            outputCollector.emit(new Values(counts));  
    	}else{
    		String word = tuple.getString(0);  
            Integer count = counts.get(word);  
            if(count == null)  
                count = 0;  
            count++;  
            counts.put(word,count);  
    	}
		outputCollector.ack(tuple);//通过ack观测器，这条数据处理成功
        
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 定义两个字段word和count
		declarer.declare(new Fields("word-count"));
	}
}