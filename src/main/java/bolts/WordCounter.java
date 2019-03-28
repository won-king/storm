package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kewangk on 2018/2/8.
 */
public class WordCounter extends BaseRichBolt{
    private OutputCollector outputCollector;
    private Map<String,Integer> count;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        count=new HashMap<>();
        int taskNum=topologyContext.getComponentTasks("count-bolt").size();
        Map<Integer,String> taskToComponent=topologyContext.getTaskToComponent();
        String componentId=topologyContext.getThisComponentId();
        int taskId=topologyContext.getThisTaskId();
        Set<String> streams= topologyContext.getThisStreams();
        List<Integer> tasks= topologyContext.getThisWorkerTasks();
        String desc=String.format("streams->%s, tasks->%s, componentId->%s, taskComponent->%s, taskId->%d",
                streams.toString(), tasks.toString(), componentId, taskToComponent.toString(), taskId);
        System.out.println(desc);
        System.out.println("-----componentId->"+topologyContext.getThisComponentId()+
                " taskId->"+topologyContext.getThisTaskId()+" taskNum->"+taskNum+"-----");
    }

    public void execute(Tuple tuple) {
        String s=tuple.getStringByField("word");
        Integer a=count.get(s);
        a= a==null? 1:a+1;
        //AtomicInteger c=count.get(s);
        //int nc=c==null? 0 : c.incrementAndGet();
        //AtomicInteger a=new AtomicInteger(nc);
        count.put(s, a);
        outputCollector.emit(new Values(s,a));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
