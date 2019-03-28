package spouts;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wangke18 on 2019/2/14.
 * 功能描述：数据源
 *    需要注意数据源的功能边界，它只提供非常基础的功能，指定位置区间的数据读取，从指定下标开始剩余可读数量
 *    它不会记录我当前数据的读取位置，也不负责记录当前数据成功处理到的位置
 *    因为可能存在多个消费者来读取同一个数据源，它们是做完全不同的计算，所以需要消费者自己维护读取位置和成功处理的位置
 */
public class MyDataSource implements Serializable{
    private static final long serialVersionUID = -8093124329418919227L;
    private static String[] sentences={"my name is soul",
            "im a boy",
            "i have a dog",
            "my dog has fleas",
            "my girl friend called finger",
            "and her wechat ID is you are my only love"};

    private static MyDataSource INSTNACE=new MyDataSource();

    private MyDataSource(){}

    public static MyDataSource instance(){
        return INSTNACE;
    }

    //获取从from开始，剩下所有的可用消息数
    public long getAvailable(long from){
        long available= getEndPoint()-from;
        return available >=0 ? available : 0;
    }

    public List<String> getMessages(long from, int quantity){
        List<String> rest=new ArrayList<>(quantity);
        int flag=0;
        while (flag < quantity) {
            rest.add(sentences[(int) (from+flag++)]);
        }
        return rest;
    }

    public long getEndPoint(){
        return sentences.length;
    }

    public void close(){
        //todo
    }

}
