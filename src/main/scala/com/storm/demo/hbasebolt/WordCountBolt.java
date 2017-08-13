package com.storm.demo.hbasebolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.storm.utils.TupleUtils;

public class WordCountBolt extends BaseRichBolt {
	Map<String, Integer[]> kv = new HashMap<String, Integer[]>();
	private OutputCollector outputCollector;
	Connection conn;
	Table table;
	String zookeeper;
	String tablename;
	public WordCountBolt(String zookeeper,String tablename){
		this.zookeeper=zookeeper;
		this.tablename=tablename;
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		//每隔1清空一次数据
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1L);// 加入Tick时间窗口，进行统计
		return conf;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
		Configuration hconf = HBaseConfiguration.create();
		hconf.set("hbase.zookeeper.quorum",zookeeper);
		hconf.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			conn = ConnectionFactory.createConnection(hconf);
			table = conn.getTable(TableName.valueOf(tablename));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			if(TupleUtils.isTick(tuple)){
				ArrayList<Put> puts = new ArrayList<Put>();
				for (String key : kv.keySet()) {
					Put put = new Put(key.getBytes());
					Integer[] value = kv.get(key);
					put.addColumn("info".getBytes(), "count".getBytes(),value[0].toString().getBytes());
					puts.add(put);
				}
				try {table.put(puts);} catch (IOException e) {e.printStackTrace();}
			}else{
				String rowkey = tuple.getString(0);
				Integer count = tuple.getInteger(1);
				Integer[] pre = kv.get(rowkey);
				if (null != pre) {
					pre[0] = pre[0] + count;
				} else {
					Get get = new Get(rowkey.getBytes());
					Result re = table.get(get);
					pre = new Integer[2];
					if (!re.isEmpty()) {
						int countV = Integer.parseInt(new String(re.getValue("info".getBytes(), "count".getBytes())));
						pre[0] = add(countV, count);
					} else {
						pre[0] = count;
					}
				}
				kv.put(rowkey, pre);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		outputCollector.ack(tuple);
	}
	public int add(int a, int b) {
		return a + b;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}