package com.heman.hdpdemo.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;


public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    protected static final String RESULT = "top_10_words";
    protected static final String TIMESTAMP = "ts";
    private static final String COMMA = ",";
    private Map<String, Long> counter;
    private long thenTime;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector)
    {
        this.collector = collector;
        counter = new HashMap<String, Long>();
        thenTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple input) {

        String word = input.getStringByField(WordsSplitterBolt.WORD_FIELD);
        if (counter.containsKey(word)) {
            long val = counter.get(word) + 1;
            counter.put(word, val);
        } else {
            counter.put(word, 1L);
        }

        //print the top 10 words in last 5 seconds
        if (System.currentTimeMillis() - thenTime > 5000) {
            String str = getTopWords(10);
            counter = new HashMap<String, Long>();
            thenTime = System.currentTimeMillis();
            collector.emit(new Values(thenTime, str));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(TIMESTAMP, RESULT));
    }

    private String getTopWords(int count) {

        List<String> list = new ArrayList<String>();
        StringBuffer result = new StringBuffer();

        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            list.add(entry.getValue() + COMMA + entry.getKey());
        }
        Collections.sort(list, Collections.reverseOrder());
        int i = 0;
        while (i < count && list.size() > i) {
            String[] temp = list.get(i).split(COMMA);
            result.append("[").append(temp[1]).append(":").append(temp[0]).append("]").append(COMMA);
            i++;
        }

        return result.toString();
    }
}