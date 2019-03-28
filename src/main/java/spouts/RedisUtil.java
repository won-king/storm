package spouts;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangke18 on 2019/2/18.
 * 功能描述：消费者负责维护的一个当前数据源读取位置，成功处理到的数据源位置下标，成功处理的事务id等
 */
public class RedisUtil {

    private static Map<BigInteger,TransactionMetaData> status=new HashMap<>();
    private static volatile long index=0;

    private static volatile BigInteger lastSuccessId;
    private static volatile TransactionMetaData lastSuccessData;

    private static MyDataSource dao=MyDataSource.instance();

    public static long getAvaliable(){
        return dao.getAvailable(index);
    }

    public static boolean available(){
        return index<dao.getEndPoint();
    }

    public static void setStatus(BigInteger transactionId, TransactionMetaData metaData){
        status.put(transactionId, metaData);
    }

    public static TransactionMetaData getMetaData(BigInteger transactionId){
        return status.get(transactionId);
    }

    public static long getIndex() {
        return index;
    }

    public static void setIndex(long index) {
        RedisUtil.index = index;
    }

    public static BigInteger getLastSuccessId() {
        return lastSuccessId;
    }

    public static void setLastSuccessId(BigInteger lastSuccessId) {
        RedisUtil.lastSuccessId = lastSuccessId;
    }

    public static TransactionMetaData getLastSuccessData() {
        return lastSuccessData;
    }

    public static void setLastSuccessData(TransactionMetaData lastSuccessData) {
        RedisUtil.lastSuccessData = lastSuccessData;
    }

    public static void close(){
        dao.close();
    }
}
