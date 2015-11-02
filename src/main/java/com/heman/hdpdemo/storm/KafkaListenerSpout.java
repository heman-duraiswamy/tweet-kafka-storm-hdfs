package com.heman.hdpdemo.storm;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.*;

public class KafkaListenerSpout {

    private static final String ZK_HOST = "localhost:2181";
    private static final String ZK_ROOT = "/twitter_event_spout";
    private static final String GROUP_ID = "TwitterStormSpout";
    protected static final String TWEET_FIELD = "tweet";

    private String KAFKA_TOPIC;
    private KafkaSpout kafkaSpout;

    public KafkaListenerSpout(String kafkaTopic) {
        this.kafkaSpout = new KafkaSpout(constructKafkaSpoutConfig());
        this.KAFKA_TOPIC = kafkaTopic;
    }

    private SpoutConfig constructKafkaSpoutConfig() {
        BrokerHosts hosts = new ZkHosts(ZK_HOST);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, KAFKA_TOPIC, "", GROUP_ID);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        return spoutConfig;
    }

    protected KafkaSpout getKafkaSpout() {
        return this.kafkaSpout;
    }
}