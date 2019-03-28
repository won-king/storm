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

        //本task对象信息
        int taskId=topologyContext.getThisTaskId();
        String self=String.format("-------taskId->%d-------", taskId);
        System.out.println(self);
        int taskNum=topologyContext.getComponentTasks("split-bolt").size();
        Map<Integer,String> taskToComponent=topologyContext.getTaskToComponent();
        String componentId=topologyContext.getThisComponentId();
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
            //为配合消息可靠性机制，必须采用锚定手法，把tuple逐个传递到整个拓扑结构
            outputCollector.emit(tuple, new Values(string));
            System.out.println("word normalizer emit->"+string);
        }
        outputCollector.ack(tuple);
        //outputCollector.fail(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
