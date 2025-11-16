import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class HbaseSparkAnalytics {

    public static void main(String[] args) {

        // ============================
        //   CONFIG HBASE + SPARK
        // ============================
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hadoop-master");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set(TableInputFormat.INPUT_TABLE, "products");

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseAnalytics")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<ImmutableBytesWritable, Result> rdd =
                sc.newAPIHadoopRDD(
                        config,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class,
                        Result.class
                );

        // ============================
        //  Extraction colonne "price"
        // ============================
        JavaRDD<Double> prices = rdd.map(t -> {
            Result r = t._2;
            byte[] val = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("price"));
            if (val == null) return 0.0;
            return Double.parseDouble(Bytes.toString(val));
        });

        // ============================
        //   1. TOTAL DES VENTES
        // ============================
        double total = prices.reduce(Double::sum);
        System.out.println("\n=== TOTAL DES VENTES ===");
        System.out.println(total);

        // ============================
        //  2. TOTAL PAR PRODUIT
        // ============================
        JavaPairRDD<String, Double> salesByProduct = rdd.mapToPair(t -> {
            Result r = t._2;

            String product = Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("product")));
            double price = Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("price"))));

            return new Tuple2<>(product, price);
        }).reduceByKey(Double::sum);

        System.out.println("\n=== TOTAL PAR PRODUIT ===");
        salesByProduct.collect().forEach(System.out::println);

        // ============================
        //   3. TOTAL PAR VILLE
        // ============================
        JavaPairRDD<String, Double> salesByCity = rdd.mapToPair(t -> {
            Result r = t._2;

            String town = Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("town")));
            double price = Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("price"))));

            return new Tuple2<>(town, price);
        }).reduceByKey(Double::sum);

        System.out.println("\n=== TOTAL PAR VILLE ===");
        salesByCity.collect().forEach(System.out::println);

        // ============================
        //   4. MOYENNE DES VENTES
        // ============================
        long count = prices.count();
        double avg = (count == 0) ? 0.0 : total / count;

        System.out.println("\n=== MOYENNE DES VENTES ===");
        System.out.println(avg);

        // ============================
        //   5. MIN & MAX DES VENTES
        // ============================
        double min = prices.min(Double::compare);
        double max = prices.max(Double::compare);

        System.out.println("\n=== MIN DES VENTES ===");
        System.out.println(min);

        System.out.println("\n=== MAX DES VENTES ===");
        System.out.println(max);

        // Fin
        sc.close();
    }
}
