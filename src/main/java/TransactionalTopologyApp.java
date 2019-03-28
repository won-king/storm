import bolts.TransactionCommitter;
import bolts.TransactionWordCollector;
import bolts.TransactionWordCounter;
import bolts.TransactionWordSplitterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.base.BasePartitionedTransactionalSpout;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import spouts.MyTransactionSpout;
import spouts.TransactionSpoutEmitter;

/**
 * Created by wangke18 on 2019/1/29.
 * 功能描述：storm事务性拓扑的一个示例
 *          storm支持所谓的事务性拓扑的目的，其实就是为了支持批处理。那为什么需要批处理呢
 *          是由于storm最初设计出来是仅支持单个消息计算的，就是来一个处理一个
 *          但是这种计算方式虽然实时性高，但效率不高，吞吐量低
 *          编程领域有一个通用的法则你记住，要想提高效率，就要善于使用批处理，能一次完成的事情，不要一个一个串行去做
 *
 * 巨坑提醒：事务性拓扑中的发射的所有tuple都必须以TransactionAttempt作为第一个field，
 *          注意是所有的，所以在declareField的时候一定注意带上
 *
 *          所有自定义的batchBolt都不要直接继承BaseBatchBolt，这个里面的泛型必须是TransactionAttempt
 *          所以batchBolt要继承BaseTransactionalBolt
 *
 * 关于消息的重发机制的一些理解：
 *   消息在什么情况下会重发？
 *   --目前来看，只有一种情况下会重发，消息处理超时。
 *     任何其他情况，你的某个bolt是拒绝处理特定的消息也好，还是丢掉特定的消息也好(如果抛了异常，整个拓扑会直接down掉)
 *     只要它们的execute方法能及时结束，都会被storm认为这个消息得到了成功的处理
 *   谁来负责重发？
 *   --目前来看，似乎是storm框架本身会记录一个事务ID与批次消息的映射，它检测到超时会自动重发，
 *     我现在在coordinator中手写的重发代码，似乎并没有起作用
 *   从哪里开始重发？
 *   --一开始理解的是从spout数据源开始重发，实际观察并非如此。是从计算开始出问题的那个节点的前一个节点开始重发。
 *     比如说，比如说，我是在counter节点模拟的超时，结果表明，消息是从splitter开始重发的，并不是coordinator(数据源)
 *   什么时候开始重发？
 *   --不是超时立即重发，而是另外配置，目前试出来的默认值貌似是5秒
 *
 * storm如何追踪整个tuple树中的所有节点都处理成功了？
 * --storm中有一个特殊的task，acker,专门用来追踪spout发射的tuple树。当它检测到树中的所有节点都处理完了，就会发出一个确认消息
 *   acker追踪算法原理：对于每一个spout发出的tuple，都会保存一个ack-val校验值，初始值是0，
 *                    每次emit或ack的时候，都要把这个值与tuple id做异或运算，用得到的结果更新这个值
 *                    如果每一个发射的tuple都被ack了，最终这个值一定是0，表明tuple树被完全处理
 *        此原理说白了，就是利用一个很简单的异或运算技巧，一个值和0异或是它本身，和它本身再异或一次，就变成0
 *        一个tuple emit一次，ack一次，校验值一来一回，最终还是会变成0，中间只要有任何一次emit没有被ack，值都不会是0
 * --一句话说明白了就是，storm不会那么傻，真的去追踪一个源tuple最终生成的tuple树是否跟拓扑结构是一致的
 *   它的追踪目标很简单，就是保证，每一个emit出来的tuple都被ack过了
 *
 * 上面说到了acker追踪算法，下面就来说说可靠性消息中，tuple锚定的作用
 * --我一开始以为，acker追踪算法和锚定是保证消息可靠性的两种实现方式，是独立的，其实我错了，它俩合起来才能实现消息可靠性担保
 *   上面说过，acker任务中会保存一个源tuple到ack-val校验值的映射
 *   在不可靠性消息中，所有节点都是无状态计算，他们只是来一个消息算一个，完了继续发射tuple
 *   计算节点根本不知道它处理的tuple是哪一个tuple树上的，这样就无法把源tuple与ack-val对应起来，无法进行tuple id的异或运算
 *   我们需要把源tuple在树中一层一层传递下去，具体来说就是用messageId来表示一个源tuple，所以实际上是messageId到ack-val的映射
 *   锚定的作用就是把源tuple对应的messageId，传递到tuple树上的每一个节点上，配合追踪算法对源tuple的ack-val值进行异或运算
 *
 * 事务性拓扑中，如何知道batch的processing阶段完成了？(需要把这个机制与上面的追踪算法做区分)
 * --storm在普通bolt的基础上，又封装了一个CoordinateBolt，用于支持批次处理完成的记录
 *   这个bolt内部主要记录两个值，我的上游是谁(哪些task给我发送了tuple)，我的下游是谁(我要给哪些task发送tuple)
 *   coordinateBolt内部会做这样一件事，上游通知我，发送了给M个tuple给我，我会把M与我这一次处理过的tuple数量做比对，
 *   如果相等，则说明到我这里，批次的所有消息都是成功处理的，我会再以emitDirect的方式，通知所有我发送过tuple的task，
 *   我这一次发送了多少个tuple给它，依次通知下去，一直到committer bolt
 *
 * 关于storm消息可靠性担保机制的一些理解
 * --storm提供了多种消息可靠性担保机制。通过对这些机制共同点的总结，我发现了一个基本公理
 *   就是所有的消息可靠性担保机制，实现方法无非就是一种，即，消息的一应一答
 *   storm会跟踪每一个tuple的生命周期，每一个被emit的tuple，只有被ack了，才会被认为成功处理
 *   当要自己设计一个可靠性系统的时候，可以借鉴上述思路
 *
 * 通常来说，storm提供三种消息担保机制，至多一次，至少一次，exactly once
 * 至多一次：其实就是普通的，没有ack机制的消息计算
 * 至少一次：就是开启ACK机制的消息计算
 * exactly once：Trident API在事务性拓扑的基础上实现的一套精确处理一次的计算框架
 *
 * 提出一个扩展思路：目前为止的例子，committer都是只有一个的，其实一个topology里面，是可以设置多个committer的
 *   这种可能出现的问题可能会更复杂，需要更多的设计来保证消息不重复计算
 */
public class TransactionalTopologyApp {
    public static void main(String[] args) {
        String BUILDER="builder";
        String SPOUT="spout";
        String SPLITTER="splitter";
        String COUNTER="counter";
        String COLLECTOR="collector";
        String COMMITTER="committer";
        TransactionalTopologyBuilder builder=new TransactionalTopologyBuilder(BUILDER,SPOUT, new MyTransactionSpout());
        builder.setBolt(SPLITTER, new TransactionWordSplitterBolt()).shuffleGrouping(SPOUT);
        builder.setBolt(COUNTER, new TransactionWordCounter())
                .fieldsGrouping(SPLITTER, new Fields("word"));
        builder.setBolt(COLLECTOR, new TransactionWordCollector())
                .globalGrouping(COUNTER);
                //.globalGrouping(SPLITTER, TransactionWordSplitterBolt.SENTENCE_STREAM);

        //这里按照预想，本来应该是一个按批次全量更新的打印输出，实际却只打印当前批中出现过的单词，但是其在整个流中的总数又是对的
        //根据调试，committer对象又只初始化过一次，这样看起来committer对象只有一个，实则不然，实际运行的时候，确实是三个不同的对象
        //由于所有组件都是可序列化的，所以实际情况可能是，最初new出的那个对象，通过序列化进行了序列化复制
        //每一批都会序列化一个新组件来处理，而且都是从最初的那个对象序列化出来的，所以每一批里的hashMap都是一个新容器
        //为了证明这个猜想，就重写了clone方法，打印输出
        //但又没有打印日志，只能说明一个问题，clone方法没有被调用
        //其实是我把clone和序列化的概念搞混了，这根本就是两种方式，clone是clone，序列化是序列化，序列化的时候并不会调clone方法
        builder.setBolt(COMMITTER, new TransactionCommitter())
                .globalGrouping(SPOUT, TransactionSpoutEmitter.METADATA_STREAM)
                .globalGrouping(COLLECTOR);


        Config config = new Config();
        config.setMessageTimeoutSecs(3);
        LocalCluster cluster = new LocalCluster();
        String TOPOLOGY_NAME="transaction";
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.buildTopology());
        Utils.sleep(15000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
