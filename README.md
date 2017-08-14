# storm demo
storm version 1.1.0
kafka version 0.8

* 提供简单的storm 的wordcount demoe
* 提供 Storm + Kafka + hbase 的统计demoe

> # storm的安装
```
* 解压文件：
tar -zxvf apache-storm-1.1.0.tar.gz
* 修改文件conf/storm.yaml
：每一个配置点前面需要加一个空格“”，否则启动时会报配置文件相关异常。
storm.zookeeper.servers:
  - "nimbus-ip"
storm.local.dir: "/mnt/storm"
nimbus.seeds: ["nimbus-ip"]
storm.zookeeper.port: 2181
ui.port: 8080
nimbus.host: "nimbus-ip"
supervisor.slots.ports:
    - 6700
    - 6701
    - 6702
    - 6703
storm.messaging.netty.max_wait_ms: 10000
* 启动 主节点
nohup ./storm nimbus  >/dev/null &
启动UI
nohup ./storm ui >/dev/null &

* 启动子节点
nohup ./storm supervisor >/dev/null &
在work节点上启动日志监控
nohup ./storm logviewer &


> storm的运行
```
./storm jar xxxx.jar xxx.xxx.xxx.xxx.WordCountTest WordCountTest

