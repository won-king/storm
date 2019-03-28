package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by kewangk on 2018/2/8.
 */
public class WordNormalizer extends BaseRichBolt {
    private OutputCollector outputCollector;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        int taskNum=topologyContext.getComponentTasks("split-bolt").size();
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
        String s=tuple.getStringByField("sentence");
        String[] words=s.split(" ");
        for(String string:words){
            outputCollector.emit(new Values(string));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
