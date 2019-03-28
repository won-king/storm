import bolts.CountReporter;
import bolts.WordCounter;
import bolts.WordNormalizer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.topology.TridentTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import spouts.WordReader;

/**
 * Created by kewangk on 2018/2/8.
 * storm并发度的理解：
 * 一个topology可以跑在多个worker上(worker其实就是一个进程，也可以说是一台机器)，一个worker只能对应于一个topology
 * 一个worker可以包含多个executor，executor其实就是线程，一个executor至少对应于一个component，
 *     一个component可以被多个executor共享，但是必须保证component数量不小于并行度，也就是executor的数量
 *     如果设置的taskNum<parallelism，则实际创建executor时，数量以taskNum为准
 *     之前理解的是错误的，说当taskNum<parallelism时，实际创建出来的taskNum以parallelism为准，这个是错误的
 *     其实就是一个以哪个参数为优先的问题，答案是taskNum。
 *     我们说的可以在不重启拓扑的情况下动态修改计算拓扑的并行度，也只能是修改parallelism的值，taskNum在代码中一旦定了，就不能改了
 * task就是具体的计算逻辑对象(可以理解为一个runnable对象)
 * 一般默认，一个executor只执行一个task，即并发度和numTask是1:1的关系
 *
 * 在实际应用中，一个component可能不仅需要关心自己数据源有哪些，还需要关心自己的订阅者有哪些
 * 因为有时候，你需要自定义某些tuple发往哪些component，这时候就需要知道你的消息订阅者的component数量，通过指定下标控制
 *
 * 还有一点要注意：如果bolt实现IRichBolt采用了自动确认机制，那么最好不要在业务逻辑中做非常耗时的操作，更不要做失败重试等
 *   因为自动确认会有超时时间的，超时则认为消息失败，会重发，而如果超时后，消息被成功处理了，重发后的再处理一遍，就会导致重复计算
 */
public class TopologyMain{
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main( String[] args ){
        WordReader spout = new WordReader();
        WordNormalizer splitBolt = new WordNormalizer();
        WordCounter countBolt = new WordCounter();
        CountReporter reportBolt = new CountReporter();

        //创建了一个TopologyBuilder实例
        //TopologyBuilder提供流式风格的API来定义topology组件之间的数据流
        TopologyBuilder builder = new TopologyBuilder();

        //支持事务的计算拓扑
        //TridentTopologyBuilder builder1=new TridentTopologyBuilder();

        //注册一个sentence spout,设置两个Executeor(线程)，默认一个
        //这里如果把spout的并发度设为2，实际上就会有2个spout对象，相当于有两个数据源在发射数据，造成统计出来的结果都是2倍的
        builder.setSpout(SENTENCE_SPOUT_ID, spout,1);

        //注册一个bolt并订阅sentence发射出的数据流，shuffleGrouping方法告诉Storm要将SentenceSpout发射的tuple随机均匀的分发给SplitSentenceBolt的实例
        //builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);

        //SplitSentenceBolt单词分割器设置4个Task，2个Executeor(线程)
        //如果并发度>numTask，则实际创建拓扑结构时，executor的数量为numTask
        //也就是说numTask是设置并发度的上限，parallelism是设置并发度的下限，
        // 大多数情况下，我们应该保证上限>=下限
        // 但是如果某个新手程序员不知道这种约定，设成了下限 > 上限，那么实际创建executor时，数量与下限保持一致，不会多创建线程
        builder.setBolt(SPLIT_BOLT_ID, splitBolt,4).setNumTasks(2).shuffleGrouping(SENTENCE_SPOUT_ID);
        //之前的想法错了，这里还是设置了几个numTasks，实际就会创建几个task对象，parallelism是多少，就会创建多少个executor
        // 但是如果numTasks<parallelism_hint，则多出来的executor会空闲
        builder.setBolt(SPLIT_BOLT_ID, splitBolt,2).setNumTasks(2).shuffleGrouping(SENTENCE_SPOUT_ID);

        // SplitSentenceBolt --> WordCountBolt

        //fieldsGrouping将含有特定数据的tuple路由到特殊的bolt实例中
        //这里fieldsGrouping()方法保证所有“word”字段相同的tuuple会被路由到同一个WordCountBolt实例中
        //builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping( SPLIT_BOLT_ID, new Fields("word"));

        //WordCountBolt单词计数器设置4个Executeor(线程)
        //allGrouping用于订阅spout发射的信号消息，从而触发相应的动作
        builder.setBolt(COUNT_BOLT_ID, countBolt,4)
                .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"))
                .allGrouping(SENTENCE_SPOUT_ID, WordReader.SIGNAL);
        //如果把拓扑看成一个树形结构，那么当storm创建拓扑结构节点的时候，是从最底层的孩子节点开始创建的，从他们的id就可以看出来
        builder.setBolt(COUNT_BOLT_ID, countBolt,2).fieldsGrouping( SPLIT_BOLT_ID, new Fields("word"));

        // WordCountBolt --> ReportBolt

        //globalGrouping是把WordCountBolt发射的所有tuple路由到唯一的ReportBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);


        Config config = new Config();//Config类是一个HashMap<String,Object>的子类，用来配置topology运行时的行为
        //设置worker数量
        //config.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();

        //本地提交
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        Utils.sleep(10000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

    }
}
