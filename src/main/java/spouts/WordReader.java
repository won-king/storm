package spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kewangk on 2018/2/8.
 */
public class WordReader extends BaseRichSpout {
    private static String[] sentences={"my name is soul",
            "im a boy",
            "i have a dog",
            "my dog has fleas",
            "my girl friend called finger"};

    private SpoutOutputCollector spoutOutputCollector;
    private int index;

    private AtomicInteger integer=new AtomicInteger(0);
    private static final int THRESHOLD=9;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
    }

    public void nextTuple() {
        //发射单词句子
        /*spoutOutputCollector.emit(new Values(sentences[index++]));
        if(index>=sentences.length){
            index=0;
        }
        Utils.sleep(1);*/

        //发射自然数序列
        if(integer.intValue()>THRESHOLD){
            this.close();
            spoutOutputCollector.emit("signal",new Values(0));
            return;
        }
        spoutOutputCollector.emit(new Values(integer.incrementAndGet(), integer.incrementAndGet()));
        /*if(integer.intValue()%2==0){
            spoutOutputCollector.emit("even",new Values(integer.incrementAndGet()));
        }else {
            spoutOutputCollector.emit("odd",new Values(integer.incrementAndGet()));
        }*/
        //Utils.sleep(500);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare(new Fields("sentence"));
        //outputFieldsDeclarer.declare(new Fields("number"));
        outputFieldsDeclarer.declare(new Fields("odd","even"));
        outputFieldsDeclarer.declareStream("signal", new Fields("stopped"));
        //outputFieldsDeclarer.declareStream("even",new Fields("number"));
        //outputFieldsDeclarer.declareStream("odd",new Fields("number"));
        //outputFieldsDeclarer.declareStream("signal", new Fields("stopped"));
    }

    @Override
    public void ack(Object msgId){
        System.out.println("message "+msgId+" is dealt successfully");
    }

    @Override
    public void fail(Object msgId){}
}
