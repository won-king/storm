package spouts;

import java.io.Serializable;

/**
 * Created by wangke18 on 2019/1/30.
 * 功能描述：用于记录当前读取的数据源的下标及数量
 *          由于消息分批之后，这一批一起计算时，其实就成了一个事务，要么都成功，要么都失败，不允许出现一两个消息失败的情况
 *          原本的数据流是连续且没有边界的，为了把它们分成一批一批的，我们需要这样一个数据结构来描述
 *          某一批数据处于原始数据流中的哪一个位置
 */
public class TransactionMetaData implements Serializable {
    private static final long serialVersionUID = 1L;

    private long from;
    private int quantity;

    public TransactionMetaData(long from, int quantity) {
        this.from = from;
        this.quantity = quantity;
    }

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "TransactionMetaData{" +
                "from=" + from +
                ", quantity=" + quantity +
                '}';
    }
}
