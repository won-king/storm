package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kewangk on 2018/2/8.
 */
public class CountReporter extends BaseRichBolt {
    private Map<String,Integer> result;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        result=new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word=tuple.getStringByField("word");
        Integer num=tuple.getIntegerByField("count");
        result.put(word,num);
        System.out.println(result);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup(){
        System.out.println("-----final result-----");
        for(Map.Entry<String,Integer> entry: result.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
    }
}
