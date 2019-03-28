package bolts;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Tuple;
import spouts.MyDataSource;
import spouts.RedisUtil;
import spouts.TransactionMetaData;
import spouts.TransactionSpoutEmitter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangke18 on 2019/2/14.
 */
public class TransactionCommitter extends BaseTransactionalBolt implements ICommitter {
    private static int num=0;
    private BatchOutputCollector collector;
    private TransactionAttempt attempt;
    private TransactionMetaData metaData;
    private int lines;
    private MyDataSource dao=MyDataSource.instance();
    //由于序列化的存在，实际运行的时候，可以理解为是每一个批次一个对象，所以这里的result存的不是全量数据，而是每批的增量数据
    //尽管可以用static来解决，但是我还没有试过，在分布式环境下能成功
    //因为在分布式环境下，保证一个类对象被序列化后的静态特性是非常困难的

    //所以这个例子告诉我们，在写计算组件的时候，尽量不要声明内部存储，要想存储一些统计量，最好使用外部分布式数据库来
    //简而言之，这里面声明的所有变量，都可以理解为是每一批计算产生的中间数据，
    // 一批计算完了，这些变量就没用了，新的一批会采用全部采用初始化的变量
    //对象的复制采用序列化的方法来实现
    private Map<String,Integer> result=new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt transactionAttempt) {
        collector=batchOutputCollector;
        //如果这里的事务id是每次重新赋值的话，那是否意味着，committer每次都是重新new对象的
        attempt=transactionAttempt;
        //result=new HashMap<>();
        //System.out.println("[committer] initial->"+num++);
    }

    @Override
    public void execute(Tuple tuple) {
        String stream=tuple.getSourceStreamId();
        if(TransactionSpoutEmitter.METADATA_STREAM.equals(stream)){
            metaData= (TransactionMetaData) tuple.getValueByField(TransactionSpoutEmitter.METADATA_STREAM);
        }else if(TransactionWordCollector.LINES_STREAM.equals(stream)){
            lines=tuple.getIntegerByField(TransactionWordCollector.LINES_STREAM);
        }else {
            String word=tuple.getStringByField("word");
            int count=tuple.getIntegerByField("count");
            result.put(word, count);
        }
    }

    @Override
    public void finishBatch() {
        //TODO 这里执行批次消息处理成功的提交工作，持久化当前批次的位置等
        //乍一看这里的工作似乎跟上面BaseBatchBolt的finishBatch里做的工作是有重复的，并且ICommitter也只是个标记接口，可以随意放置
        //明天再具体研究一下，看到底多一个缓冲节点有没有必要
        /*if(dao.getIndex()>3){
            dao.setNext(4);
            return;
        }*/
        System.out.println("[committer] currentBatch->"+metaData);
        System.out.println("[committer] attempt->"+attempt);
        for(Map.Entry<String,Integer> entry:result.entrySet()){
            System.out.println("[committer] "+entry.getKey()+"->"+entry.getValue());
        }
        RedisUtil.setLastSuccessId(attempt.getTransactionId());
        System.out.println("--------------------");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

}
