package bolts;

import org.apache.storm.spout.ISpout;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by kewangk on 2018/2/9.
 */
public class Accumulator extends BaseRichBolt{
    private long sum=0;
    private OutputCollector outputCollector;
    private boolean completed;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        completed=false;
    }

    @Override
    public void execute(Tuple tuple) {
        if("number-stopper".equals(tuple.getSourceComponent())){
            if(!completed){
                outputCollector.emit(new Values(sum));
                completed=true;
            }
            return;
        }
        /*if(tuple.getSourceStreamId().equals("stopped")){
            System.out.println("-----spout stopped-----");
            return;
        }*/
        int num=tuple.getIntegerByField("even");
        sum+=num;
        System.out.println(Thread.currentThread().getId()+": num->"+num+" sum->"+sum);
        //outputCollector.emit(new Values(sum));
        //outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sum"));
    }
}
