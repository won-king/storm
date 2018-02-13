import bolts.Accumulator;
import bolts.Collector;
import bolts.Stopper;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import spouts.WordReader;

/**
 * Created by kewangk on 2018/2/9.
 */
public class NumberAccumulateMain {
    private static final String SPOUT_ID="number-generator";
    private static final String ACCUMULATOR_ID="number-accumulator";
    private static final String ACCUMULATOR_ID1="number-accumulator1";
    private static final String COLLECTOR_ID="number-collector";
    private static final String TOPOLOGY_NAME="number-count";
    private static final String STOPPER="number-stopper";

    public static void main(String[] args) {
        WordReader spout=new WordReader();
        Accumulator accumulator=new Accumulator();
        Collector collector=new Collector();
        Stopper stopper=new Stopper();

        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout);
        builder.setBolt(STOPPER, stopper).allGrouping(SPOUT_ID,"signal");
        //builder.setBolt(ACCUMULATOR_ID, accumulator,2).setNumTasks(2).shuffleGrouping(SPOUT_ID);
        //builder.setBolt(ACCUMULATOR_ID, accumulator,1).setNumTasks(1).fieldsGrouping("even", new Fields("number"));
        //builder.setBolt(ACCUMULATOR_ID1, new Accumulator(),1).setNumTasks(1).directGrouping(SPOUT_ID);
        builder.setBolt(ACCUMULATOR_ID, new Accumulator(),2).setNumTasks(2)
                .fieldsGrouping(SPOUT_ID, new Fields("odd","even"))
                .allGrouping(STOPPER);
        /*builder.setBolt(ACCUMULATOR_ID1, new Accumulator(),1).setNumTasks(1)
                .fieldsGrouping(SPOUT_ID, new Fields("even"));*/
                //.allGrouping(STOPPER);
        builder.setBolt(COLLECTOR_ID, collector).globalGrouping(ACCUMULATOR_ID);

        Config config=new Config();
        config.setDebug(false);
        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        Utils.sleep(1000*3);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
