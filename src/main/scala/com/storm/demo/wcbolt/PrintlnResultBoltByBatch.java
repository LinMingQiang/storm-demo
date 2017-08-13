package com.storm.demo.wcbolt;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class PrintlnResultBoltByBatch extends BaseRichBolt {
	Connection conn;
	Table table;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Map<String, Integer> word_count = (Map<String, Integer>)input.getValue(0);
		for(Entry<String, Integer>  e :word_count.entrySet()){
			System.out.println(e.getKey()+" - "+e.getValue());
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		System.out.println("#####################################333");
		super.cleanup();
	}

}
