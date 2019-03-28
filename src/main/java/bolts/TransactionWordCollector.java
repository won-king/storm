package bolts;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import spouts.TransactionMetaData;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangke18 on 2019/2/14.
 */
public class TransactionWordCollector extends BaseTransactionalBolt {
    public static final String LINES_STREAM="lines";
    private static final int MAX_FAIL_TIMES=3;

    private static int num=0;
    private static int failTimes=0;
    private Map<String,Integer> result=new HashMap<>();
    private int lines=0;
    private BatchOutputCollector collector;
    private TransactionAttempt attempt;
    private TransactionMetaData metaData;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt o) {
        collector=batchOutputCollector;
        //result=new HashMap<>();
        attempt=o;
        //System.out.println("[collector] initial->"+num++);
    }

    @Override
    public void execute(Tuple tuple) {
        String stream=tuple.getSourceStreamId();
        if(TransactionWordSplitterBolt.SENTENCE_STREAM.equals(stream)){
            String sentence=tuple.getStringByField(TransactionWordSplitterBolt.SENTENCE_STREAM);
            System.out.println("[collector] sentence->"+sentence);
            lines++;
        }else {
            //attempt= (TransactionAttempt) tuple.getValueByField("transactionId");
            metaData= (TransactionMetaData) tuple.getValueByField("metaData");
            String word=tuple.getStringByField("word");
            Integer num=tuple.getIntegerByField("count");
            result.put(word,num);
            //System.out.println("[collector] metaData->"+metaData);
            System.out.println("[collector] "+word+"->"+num);
        }
    }

    @Override
    public void finishBatch() {
        //TODO 这里进行批次计算的结尾工作，并且作为提交者的过度节点，将一批消息计算的最终结果发给提交者
        for(Map.Entry<String,Integer> entry:result.entrySet()){
            //这里本来准备测试一下，批次失败，是以什么方式来通知storm框架重新重新计算
            //一开始我以抛异常的方式来测试，结果整个计算拓扑突然挂掉了(这告诉我们整个计算拓扑中的所有代码都不能有任何异常抛出)
            //storm支持的所谓的一次且仅一次的事务语义，是依赖于数据源和提交者bolt的相互配合实现的
            //也就是说需要我们自己写代码实现，具体实现如下
            //对于数据源，最重要的前提是，我可以取到任意两个下标中间的数据集，
            // 并且需要持久化保存一个transactionId到批次元数据的映射，以便可以随时根据失败的事务id找到这批数据重新发送
            //对于提交bolt，需要持久化当前的当前最近一次处理成功的transactionId
            /*if("girl".equals(entry.getKey())){
                if((++failTimes) < MAX_FAIL_TIMES){
                    System.out.println("[collector] batch failed times->"+failTimes);
                    return;
                    //throw new RuntimeException("[collector] batch failed times->"+failTimes);
                }
            }*/
            collector.emit(new Values(attempt,metaData, entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare(new Fields("metaData","word","count"));
        outputFieldsDeclarer.declare(new Fields("transactionId","metaData","word","count"));
    }
}
