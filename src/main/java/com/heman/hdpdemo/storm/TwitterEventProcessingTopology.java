package com.heman.hdpdemo.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.kafka.*;

public class TwitterEventProcessingTopology {

    private static final String SPOUT_ID = "KafkaSpout";
    private static final String BOLT1_ID = "WordSplitBolt";
    private static final String BOLT2_ID = "WordCountBolt";
    private static final String BOLT3_ID = "HdfsBolt";
    private static final String TOPOLOGY_NAME = "Twitter-Event-Topology";

    public static void main(String[] args) throws Exception {
        System.out.println("Hello Tweets topology!!!");

        if (args.length != 1) {
            System.out.println("Usage: TruckEventsProducer <kafka_topic_name>");
            System.exit(-1);
        }

        TwitterEventProcessingTopology topology = new TwitterEventProcessingTopology();
        topology.setUpTopology(args[0]);
    }

    private void setUpTopology(String topic) throws Exception {

        Config stormConfig = new Config();
        stormConfig.setDebug(true);
        stormConfig.setMessageTimeoutSecs(60);

        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts hosts = new ZkHosts("localhost:2181");
        SpoutConfig config = new SpoutConfig(hosts, topic, "", "StormSpout");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(config);
        builder.setSpout(SPOUT_ID, kafkaSpout);

        //builder.setSpout(SPOUT_ID, new KafkaListenerSpout(args[0]).getKafkaSpout());
        builder.setBolt(BOLT1_ID, new WordsSplitterBolt()).shuffleGrouping(SPOUT_ID);
        builder.setBolt(BOLT2_ID, new WordCountBolt()).shuffleGrouping(BOLT1_ID);
        builder.setBolt(BOLT3_ID, configureHdfsBolt()).shuffleGrouping(BOLT2_ID);

        StormSubmitter.submitTopology(TOPOLOGY_NAME, stormConfig, builder.createTopology());
    }

    private HdfsBolt configureHdfsBolt() {

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

        //specifies how often to sync data to hdfs
        SyncPolicy syncPolicy = new CountSyncPolicy(10);

        // specifies when to create new file
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/twitterDemo").withPrefix("twitterWordCount");

        return new HdfsBolt()
                .withFsUrl("hdfs://sandbox.hortonworks.com:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
    }
}