package com.heman.hdpdemo.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WordsSplitterBolt extends BaseRichBolt {

    private OutputCollector collector;
    protected final static String WORD_FIELD = "word";
    private static final Set<String> FILTER_LIST = new HashSet<String>(Arrays.asList(new String[]{
            "that", "like", "have", "this", "just", "with", "about", "your", "what", "when", "where", "were",
            "want", "will", "know", "from", "would", "could", "should"
    }));

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String[] words = input.toString().split("\\|")[4].toLowerCase().split(" ");
        for (String word : words) {
            if (!isFilteredWords(word)) {
                collector.emit(new Values(word));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(WORD_FIELD));
    }

    private boolean isFilteredWords(String a) {

        if (a.length() < 4 || a.contains("http") || FILTER_LIST.contains(a)) {
            return true;
        }

        return false;
    }

}