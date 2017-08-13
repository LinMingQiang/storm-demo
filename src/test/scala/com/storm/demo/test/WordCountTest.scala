package com.storm.demo.test
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.StormSubmitter
import com.storm.demo.wcbolt.RandomSentenceSpout
import com.storm.demo.wcbolt.WordCountBolt
import com.storm.demo.wcbolt.SplitSentenceBolt
import org.apache.storm.kafka.ZkHosts
import org.apache.storm.kafka.SpoutConfig
import org.apache.storm.kafka.KafkaSpout
import org.apache.storm.spout.SchemeAsMultiScheme
import org.apache.storm.kafka.StringScheme
import scala.collection.JavaConverters._
object WordCountTest {
  def main(args: Array[String]): Unit = {
    runKafkaSpout
  }
  def run(){
    val builder = new TopologyBuilder();
    builder.setSpout("RandomSentenceSpout", new RandomSentenceSpout(), 5);
    builder
      .setBolt("SplitSentenceBolt", new SplitSentenceBolt(" "), 8)
      .shuffleGrouping("RandomSentenceSpout");
    builder
      .setBolt("WordCountBolt", new WordCountBolt(), 12)
      .fieldsGrouping("SplitSentenceBolt", new Fields("word")); //按上面的word字段来分组
    //Config.WORKER_HEAP_MEMORY_MB 和 Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB 指定，默认为 768M，另外再加上默认 64M 的 logwritter 进程内存空间，则有 832M
    val conf = new Config();
    conf.setDebug(false);
    conf.setNumWorkers(10);
    conf.setMaxSpoutPending(100000);
    //conf.setNumAckers(0); //是否启动 失败重发机制。设置为0就是不启用。在kafka里面。kafka只有成功了才回记录offset
    conf.setMessageTimeoutSecs(10000);
    // local模式 
    val cluster = new LocalCluster();
    cluster.submitTopology("WordCountTest", conf, builder.createTopology());

    //集群模式
    //StormSubmitter.submitTopology("WordCountTest", conf, builder.createTopology());  
  }
  /**
   * 从kafka里读数据
   * 单个topic
   */
  def runKafkaSpout(){
    val builder = new TopologyBuilder();
    val zk = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
    val deliverytopic = "smartadsdeliverylog"
    val deliveryzkRoot = "/storm"
    val dgroupid = "smartadsdeliverylog"
    val brokerHosts = new ZkHosts(zk);
    val dspoutConf = new SpoutConfig(brokerHosts, deliverytopic, deliveryzkRoot, dgroupid);
    dspoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    dspoutConf.zkServers = Array("solr2.zhiziyun.com", "solr1.zhiziyun.com", "mongodb3").toList.asJava
    dspoutConf.zkPort = 2181
    dspoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime; //如果groupid不存在，就从最新开始
    //设置输入
    builder.setSpout("KafkaSpout", new KafkaSpout(dspoutConf), 10); // Kafka我们创建了一个10分区的Topic，这里并行度设置为5
   
    builder
      .setBolt("SplitSentenceBolt", new SplitSentenceBolt(","), 8)
      .shuffleGrouping("KafkaSpout");
    builder
      .setBolt("WordCountBolt", new WordCountBolt(), 12)
      .fieldsGrouping("SplitSentenceBolt", new Fields("word")); //按上面的word字段来分组

    val conf = new Config();
    conf.setDebug(false);
    conf.setNumWorkers(10);
    conf.setMaxSpoutPending(100000);
    //conf.setNumAckers(0); //是否启动 失败重发机制。设置为0就是不启用。在kafka里面。kafka只有成功了才回记录offset
    conf.setMessageTimeoutSecs(10000);
    // local模式 
    val cluster = new LocalCluster();
    cluster.submitTopology("WordCountTest", conf, builder.createTopology());

    //集群模式
    //StormSubmitter.submitTopology("WordCountTest", conf, builder.createTopology());  
  }
}