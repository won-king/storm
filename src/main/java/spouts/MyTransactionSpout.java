package spouts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * Created by wangke18 on 2019/2/14.
 */
public class MyTransactionSpout extends BaseTransactionalSpout<TransactionMetaData>{

    @Override
    public Coordinator<TransactionMetaData> getCoordinator(Map map, TopologyContext topologyContext) {
        return new TransactionSpoutCoordinator();
    }

    @Override
    public Emitter<TransactionMetaData> getEmitter(Map map, TopologyContext topologyContext) {
        return new TransactionSpoutEmitter();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("transactionId", "messageId","metaData","message"));
        outputFieldsDeclarer.declareStream(TransactionSpoutEmitter.METADATA_STREAM,
                new Fields("transactionId",TransactionSpoutEmitter.METADATA_STREAM));
    }
}
