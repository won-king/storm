package spouts;

import org.apache.storm.transactional.ITransactionalSpout;

import java.math.BigInteger;

/**
 * Created by wangke18 on 2019/2/14.
 */
public class TransactionSpoutCoordinator implements ITransactionalSpout.Coordinator<TransactionMetaData> {
    private static final int MAX_BATCH_SIZE=2;

    @Override
    public TransactionMetaData initializeTransaction(BigInteger bigInteger, TransactionMetaData transactionMetaData) {
        //这个方法的作用是生成下一个批次
        //第一个参数是即将生成的批次的事务ID，第二个参数是前一个批次元数据
        System.out.println("[coordinator] transactionId->"+bigInteger);
        //这里代码的本意是为了支持批次重发，其实真正的批次重发，是从emitter那开始的，并且是storm自动完成的，这里是多此一举
        //事务性拓扑要求的数据源支持所谓的批次重发，仅仅是指，能读取任意指定位置区间数据的能力
        //所以redis中也就没必要存那么多冗余的状态数据了，但如果业务有需求还是要的，一切根据业务来
        /*TransactionMetaData exists=RedisUtil.getMetaData(bigInteger);
        if(exists!=null){
            System.out.println("[coordinator] will resend->"+transactionMetaData);
            return exists;
        }*/
        long from = transactionMetaData==null ? 0 : transactionMetaData.getFrom()+transactionMetaData.getQuantity();
        long quantity=Math.min(MAX_BATCH_SIZE, RedisUtil.getAvaliable());
        return new TransactionMetaData(from, (int) quantity);
    }

    @Override
    public boolean isReady() {
        //注：此方法会在initializeTransaction之前被调用，为了确定数据源是可用的
        return RedisUtil.available();
    }

    @Override
    public void close() {
        RedisUtil.close();
    }
}
