package bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import spouts.TransactionMetaData;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by wangke18 on 2019/2/14.
 *
 */
public class TransactionWordCounter implements IBasicBolt {

    private static final int MAX_FAIL_TIME=1;
    private static int num=0;
    private Map<String,Integer> count;
    private static int failTime=0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        System.out.println("[counter] initial->"+num++);
        count=new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        TransactionAttempt attempt= (TransactionAttempt) tuple.getValueByField("transactionId");
        TransactionMetaData metaData= (TransactionMetaData) tuple.getValueByField("metaData");
        System.out.println("[counter] received->"+tuple.getStringByField("word"));
        if(metaData.getFrom()>3 && (++failTime)<=MAX_FAIL_TIME){
            //通过超时的方式来触发消息失败重发
            //TODO 经测试，成功触发消息的重发
            //另外，通过抛出FailedException，也可以触发批次消息的重发，而不至于使整个worker进程挂掉
            //不过说起来我还应该感谢我一开始就没有使用这种方法，否则就不会大费周章的探索重发机制，得出下面的这么多结论
            //这里我一开始理解的发送的总次数，是MAX_FAIL_TIME+1次，实际上只有MAX_FAIL_TIME次，原因如下
            //是因为，我重发的次数跟你人为限定的MAX_FAIL_TIME+1没有直接关系
            //怎么理解？
            //storm重发消息，是超时自动触发的，也就是说，你这个方法里阻塞了多少秒，是超时时间的多少倍，就会重发多少次
            //举个例子，我把超时时间设为了一秒，这里阻塞了4秒，其实就重发了3次(不是4次是因为计时并没有非常精确)
            //这种情况下，MAX_FAIL_TIME是1，第二次发的时候，其实就已经不再阻塞了。
            //这样看来，其实真实的重发次数是无法准确得知的。你设置的MAX_FAIL_TIME并不能限制住重发次数，
            // 要保证所有消息处理一次且仅一次，还需要做非常多的工作，记录非常多的中间状态
            //TODO 由于重发机制的存在，所有消息都可能会被重复计算，
            // 重发机制，仅仅保证的是，一个批次被处理且仅处理一次，并不保证批次中的单个tuple仅被处理一次，需要开发者来保证
            // 具体做法就是，每一个组件，都保存一个已处理过的tuple的集合，可以根据业务场景再保存一下其所属的事务id

            //但是还有一个问题，怎么解释，当MAX_FAIL_TIME=1时，超时时间是3s，这里睡4秒，为什么实际上还是只发了一次消息就成功了
            //更奇怪的是，线程睡醒之后，是直接返回的，消息在没有重发的情况下，竟然依然走到了后面的处理流程
            //解释来了。权威解释，实践证明。（测试环境是用的单线程节点，多线程节点分析会更复杂，但是单线程中的结论是对的）
            // 先来一步步分析，首先来理一理tuple树模型，counter里接收的tuple，是单个的word，
            // 它是上一个节点发出的众多消息中的一条，而上一个节点接收的消息，又是当前批次中的一条，
            // 简单来说，counter中的一条消息，是一批消息中的某一条消息派生出的，只要这一条消息出错，就会导致整个批次重发
            // 首先第一个单词my阻塞住了，3s后，这个消息超时。
            // 重点来了。但是，超时后storm并不会立即重发消息，而是会再等待一个超时时间段，
            // 也就是说，一条消息从失败到重发下一批，会等待，超时时间x2，这么长的时间段
            // (实际观察发现，第一次重发的时间是，超时时间x2-1，原因可能是)
            // 而在此期间，如果这批消息的剩下的派生消息得到了完全的成功处理(完全的意思是，剩下的消息都被成功提交了)，都不会再重发了
            // 这种机制会导致的一个问题就是，第一个阻塞的消息，丢失了(本例中是第一个my，所以最后emitter中my的计数少1个)
            // 虽然超时了，但是后面的word消息依然能得到处理，因为本例中，counter是单线程的，所有word都得按顺序一个一个来

            //而且这也能完美解释，为什么这里睡眠5s，超时3s，能发出2条消息
            //超时3s，等6s，睡醒后，还剩1s处理剩下的所有消息，但是1s不够剩下的消息完全被处理
            //睡眠4s的情况，还剩2s处理，足够处理完剩下的消息，所以不会重发了

            //还有一个现象，睡眠5s,超时3s，如果说剩下1s处理不完的话，那么按照预想，提高并发量应该就可以了
            //所以我把counter，collector并发度都设为2，
            // 结果还是发出了2条消息，并且第一次发出的，看起来马上就都处理完了，按照上面的结论，不应该重发的
            // 看起来马上就都处理完了，其实是另一个线程(未被阻塞)中处理的那些word，并不是全部的
            // 第一个线程由于阻塞，还是触发了重发，所以导致了这个现象

            //总结下来，由一批消息派生出的所有子消息，一旦存在一个子消息超时，就会导致整批重发
            try {
                TimeUnit.SECONDS.sleep(4);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }
        String s=tuple.getStringByField("word");
        Integer a=count.get(s);
        a= a==null ? 1:a+1;
        count.put(s, a);
        //basicOutputCollector.emit(new Values(metaData,s,a));
        basicOutputCollector.emit(new Values(attempt,metaData,s,a));
        System.out.println("[counter] "+s+"->"+a);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare(new Fields("metaData","word","count"));
        outputFieldsDeclarer.declare(new Fields("transactionId","metaData","word","count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
