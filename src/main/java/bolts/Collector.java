package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by kewangk on 2018/2/9.
 */
public class Collector extends BaseRichBolt {
    private long result=0;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals("stopped")){
            System.out.println("-----spout stopped-----");
            return;
        }
        long sum=tuple.getLongByField("sum");
        System.out.println("[collector] sum->"+sum);
        result+=sum;
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup(){
        System.out.println("-----final result-----");
        System.out.println("result="+result);
    }
}
