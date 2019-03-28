package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import spouts.WordReader;

import java.util.ArrayList;
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
    private int taskId;

    private List<Tuple> batchAnchor=new ArrayList<>(5);

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        count=new HashMap<>();
        taskId=topologyContext.getThisTaskId();
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

        //如果是flush信号，则执行flush信号的逻辑
        if(WordReader.SIGNAL.equals(tuple.getSourceStreamId())){
            String signal=tuple.getStringByField(WordReader.SIGNAL);
            System.out.println("[signal ] command->"+signal+" taskId->"+taskId);
            //outputCollector.emit(batchAnchor, new Values(s,a));
            //outputCollector.ack(tuple);
            //batchAnchor.clear();
            return;
        }

        String s=tuple.getStringByField("word");
        Integer a=count.get(s);
        a= a==null ? 1:a+1;
        count.put(s, a);
        //outputCollector.emit(tuple, new Values(s,a));
        //outputCollector.ack(tuple);

        if(batchAnchor.size()==5){
            //这是对数据进行聚合的手法，即积累一个批次的数据后，对其进行reduce操作，聚合成一条数据
            //这个例子可能不太合适，因为这里没有涉及到对批次数据进行聚合的操作
            outputCollector.emit(batchAnchor, new Values(s,a));
            //这里还需要对批次的所有tuple进行ack
            for(Tuple tuple1:batchAnchor){
                outputCollector.ack(tuple1);
            }
            batchAnchor.clear();
        }else {
            batchAnchor.add(tuple);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
