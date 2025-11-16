import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HbaseSparkProcess {

    public void runJob() {

        // Configuration HBase
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hadoop-master");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        // Table HBase
        config.set(TableInputFormat.INPUT_TABLE, "products");

        // Spark config
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseRead")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Lire la table HBase comme RDD
        JavaPairRDD<ImmutableBytesWritable, Result> rdd =
                sc.newAPIHadoopRDD(
                        config,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class,
                        Result.class
                );

        System.out.println("Nombre de lignes dans products = " + rdd.count());

        sc.close();
    }

    public static void main(String[] args) {
        HbaseSparkProcess p = new HbaseSparkProcess();
        p.runJob();
    }
}
