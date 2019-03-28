package spouts;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Values;

import java.math.BigInteger;
import java.util.List;

/**
 * Created by wangke18 on 2019/2/14.
 */
public class TransactionSpoutEmitter implements ITransactionalSpout.Emitter<TransactionMetaData> {

    public static final String METADATA_STREAM="meta-data";

    private MyDataSource dao=MyDataSource.instance();

    @Override
    public void emitBatch(TransactionAttempt transactionAttempt, TransactionMetaData transactionMetaData, BatchOutputCollector batchOutputCollector) {
        List<String> strings=dao.getMessages(transactionMetaData.getFrom(), transactionMetaData.getQuantity());
        long messageId=transactionMetaData.getFrom();
        for(String s:strings){
            batchOutputCollector.emit(new Values(transactionAttempt, messageId, transactionMetaData, s));
            messageId++;
        }
        batchOutputCollector.emit(METADATA_STREAM, new Values(transactionAttempt,transactionMetaData));
        //是在这里维护下一批次的读取位置
        //dao.setNext(transactionMetaData.getFrom()+transactionMetaData.getQuantity());
        RedisUtil.setStatus(transactionAttempt.getTransactionId(), transactionMetaData);
        RedisUtil.setIndex(transactionMetaData.getFrom()+transactionMetaData.getQuantity());
        System.out.println("[emitter] transaction->"+transactionAttempt+" metaData->"+transactionMetaData);
    }

    @Override
    public void cleanupBefore(BigInteger bigInteger) {
    }

    @Override
    public void close() {
    }
}
