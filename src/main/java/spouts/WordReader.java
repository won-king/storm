package spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
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

    private static final int MAX_FAIL_TIME=5;

    public static final String SIGNAL="signal";

    private Map<Integer,Integer> failTimes=new HashMap<>(2);
    private SpoutOutputCollector spoutOutputCollector;
    private int index;

    private AtomicInteger integer=new AtomicInteger(0);
    private static final int THRESHOLD=9;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
    }

    //这个方法会被某个线程一直调用，直到worker被停止
    //这意味着，方法的一次执行，发送的其实是一批数据，这一批数据发射完成后，ack方法会被调用，确认每一个事务都完成了
    public void nextTuple() {
        //发射单词句子

        if(index>=sentences.length){
            //index=0;
            //System.out.println("------- data run out -------");
            spoutOutputCollector.emit(SIGNAL, new Values("flush"));
            Utils.sleep(3000);
            return;
        }
        String sentence=sentences[index++];
        //根据打印输出可以证明，这里的emit出的每一条消息，都是被异步处理的
        // 不会等到nextTuple方法一次执行完毕，所有消息都发射出去，下一个节点才开始接收并处理消息
        //这里添加一个消息id，是为了配合后面的bolt进行锚定
        spoutOutputCollector.emit(new Values(sentence), index-1);
        System.out.println("spout emit->"+sentence);
        Utils.sleep(200);

        //发射自然数序列
        /*if(integer.intValue()>THRESHOLD){
            this.close();
            spoutOutputCollector.emit("signal",new Values(0));
            return;
        }
        spoutOutputCollector.emit(new Values(integer.incrementAndGet(), integer.incrementAndGet()));*/
        /*if(integer.intValue()%2==0){
            spoutOutputCollector.emit("even",new Values(integer.incrementAndGet()));
        }else {
            spoutOutputCollector.emit("odd",new Values(integer.incrementAndGet()));
        }*/
        //Utils.sleep(500);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
        //outputFieldsDeclarer.declare(new Fields("number"));
        //outputFieldsDeclarer.declare(new Fields("odd","even"));
        outputFieldsDeclarer.declareStream(SIGNAL, new Fields(SIGNAL));
        //outputFieldsDeclarer.declareStream("even",new Fields("number"));
        //outputFieldsDeclarer.declareStream("odd",new Fields("number"));
        //outputFieldsDeclarer.declareStream("signal", new Fields("stopped"));
    }

    @Override
    public void ack(Object msgId){
        //只有当emit的时候，传了messageId，这里才会调用ack或fail，来确认这个消息是处理成功还是失败，
        // 如果未传，则消息messageId是null，表明此消息是无状态的，开发者不关心他是成功还是失败
        int idx= (int) msgId;

        String sentence=sentences[idx];
        System.out.println("message "+sentence+" is dealt successfully");
    }

    @Override
    public void fail(Object msgId){
        //通过这个demo，终于知道了storm是如何保证消息的可靠性的了
        //之前的模糊认识确实是对的，storm本身并不保证消息的可靠性，只提供保证可靠性的编程框架，开发者需要在此框架之内，自己实现消息的可靠性
        //具体来说，就是通过ack/fail消息确认机制，自己记录哪些消息是处理成功的，哪些失败了，手动将失败的消息游标回滚，从而实现消息重发
        //有非常多的实现方法。可以配合上游的消息队列，redis，或者直接在内存中维护一个失败消息队列，等多种方法来实现消息失败重发
        int idx= (int) msgId;
        String sentence=sentences[idx];
        Integer fail=failTimes.get(idx);
        //失败的消息不能无限重发，需要设置一个阈值，重试次数超过后不再重试
        if(fail==null || fail<=MAX_FAIL_TIME){
            System.out.println("*********message "+sentence+" failed. All sentences after that will be resent");
            index=idx;
            failTimes.put(idx, fail==null ? 1:fail+1);
        }else {
            System.out.println("message ["+sentence+"] failed too many times. will be discarded");
        }

    }
}
