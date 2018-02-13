package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kewangk on 2018/2/8.
 */
public class WordCounter extends BaseRichBolt{
    private OutputCollector outputCollector;
    private Map<String,AtomicInteger> count;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        count=new HashMap<>();
    }

    public void execute(Tuple tuple) {
        String s=tuple.getStringByField("word");
        AtomicInteger c=count.get(s);
        int nc=c==null? 0 : c.incrementAndGet();
        AtomicInteger a=new AtomicInteger(nc);
        count.put(s, a);
        outputCollector.emit(new Values(s,a.get()));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
