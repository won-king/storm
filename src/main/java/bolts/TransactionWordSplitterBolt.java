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

import java.util.Date;
import java.util.Map;

/**
 * Created by wangke18 on 2019/2/14.
 */
public class TransactionWordSplitterBolt implements IBasicBolt {
    public static final String SENTENCE_STREAM="sentence";

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        TransactionAttempt attempt = (TransactionAttempt) tuple.getValueByField("transactionId");
        TransactionMetaData metaData= (TransactionMetaData) tuple.getValueByField("metaData");
        long messageId = tuple.getLongByField("messageId");
        String message=tuple.getStringByField("message");
        System.out.println("[splitter] attempt->"+attempt+" messageId->"+messageId+" message->"+message+
                " time->"+new Date());
        String[] words=message.split(" ");
        //basicOutputCollector.emit(SENTENCE_STREAM, new Values(message));
        for(String string:words){
            basicOutputCollector.emit(new Values(attempt,metaData,string));
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("transactionId","metaData","word"));
        //outputFieldsDeclarer.declareStream(SENTENCE_STREAM, new Fields(SENTENCE_STREAM));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
