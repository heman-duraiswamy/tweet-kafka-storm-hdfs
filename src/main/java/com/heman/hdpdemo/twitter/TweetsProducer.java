package com.heman.hdpdemo.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.json.JSONObject;

import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TweetsProducer {

    private static final String CONSUMER_KEY = "bmkeeuiLqGZzBukxcPCMqUQjn";
    private static final String CONSUMER_SECRET = "Fkw9jQYPG8VT6skSOGmMk34DdVzWpolUcgofTgROvb4vAndmL3";
    private static final String ACCESS_TOKEN = "87054086-KMyQNo0A7f25LkLIAW6eX5zg0q1GPZiPved85Wvig";
    private static final String ACCESS_TOKEN_SECRET = "1HoHd1E7V2aJXH6m3mNO5dRLi7RJAnEsVddsG3ioTgLkf";

    private static String KAFKA_BROKER_LIST;
    private static String KAFKA_TOPIC;
    private static String ZK_URL;

    public static void main(String[] args) {
        System.out.println("Hello Tweets!!");

        if (args.length != 3) {
            System.out.println("Usage: TruckEventsProducer <broker_list> <kafka_topic> <zookeeper_uri>");
            System.exit(-1);
        }
        KAFKA_BROKER_LIST = args[0];
        KAFKA_TOPIC = args[1];
        ZK_URL = args[2];

        TweetsProducer.run();
    }

    private static void run() {

        // Setup Kafka
        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_BROKER_LIST);
        props.put("zk.connect", ZK_URL);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        BasicClient client = null;

        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        List<String> langList = new ArrayList<String>();
        langList.add("en");
        endpoint.languages(langList);
        endpoint.stallWarnings(false);

        try {

            Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

            client = new ClientBuilder()
                    .name("Heman_HDP_Demo")
                    .hosts(Constants.STREAM_HOST)
                    .endpoint(endpoint)
                    .authentication(auth)
                    .processor(new StringDelimitedProcessor(queue))
                    .build();

            client.connect();

            while (!client.isDone()){

                System.out.println("polling for new set of messages....");
                String msg = queue.poll(5, TimeUnit.SECONDS);

                if (null != msg && msg.contains("created_at")) {
                    StringBuilder event = new StringBuilder();
                    JSONObject json = new JSONObject(msg);
                    try {
                        if (json.has("timestamp_ms") && json.has("user") && json.has("text")) {
                            event.append(json.get("timestamp_ms").toString()).append("|")
                                    .append(json.getJSONObject("user").get("id_str").toString()).append("|")
                                    .append(json.getJSONObject("user").get("name").toString()).append("|")
                                    .append(json.getJSONObject("user").get("screen_name").toString()).append("|")
                                    .append(json.get("text").toString()
                                            .replaceAll("|", "").replaceAll("\n","").replaceAll("\r",""));
                        }

                        System.out.println(event.toString());
                        producer.send(new KeyedMessage<String,String>(KAFKA_TOPIC, event.toString()));

                    } catch (Exception e) {
                        //do nothing - ignore the message and proceed
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (client != null) {
                client.stop();
                System.out.println("Total processed messages: " + client.getStatsTracker().getNumMessages());
            }
            producer.close();
        }
    }
}