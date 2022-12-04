package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class GroupByKeyAndCountByKey {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("appName").set("spark.driver.host","localhost").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        groupBy(sc);
    }

    static void groupBy(JavaSparkContext sc) {
        List<Integer> datas = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        JavaPairRDD<Object, Iterable<Integer>> pairRDD = sc.parallelize(datas).groupBy(v1->(v1 % 2 == 0) ? "A" : "B");
        List<Tuple2<Object, Iterable<Integer>>> list = pairRDD.collect();
        for (Tuple2 t : list) {
            System.out.println(t._1 + "======" + t._2);
        }

        System.out.println("==========================================================================");



        List<String> datas2 = Arrays.asList("dog", "tiger", "lion", "cat", "spider", "eagle");

        JavaPairRDD<Integer, String> pairRDD2 = sc.parallelize(datas2).keyBy(v->v.length());
        System.out.println("pairRDD2");
        pairRDD2.collect().forEach(x->System.out.println(x));System.out.println("finpairRDD2");
        JavaPairRDD<Integer, Iterable<Tuple2<Integer, String>>> pairRDD3 = pairRDD2
                .groupBy(v->v._1);

        List<Tuple2<Integer, Iterable<Tuple2<Integer, String>>>> list2 = pairRDD3.collect();

        for (Tuple2 t : list2) {
            System.out.println(t._1 + "======" + t._2);
        }

        // countByKey
        List<Tuple2<Integer, String>> tuples = Arrays.asList(new Tuple2<Integer, String>(1, "a"),
                new Tuple2<Integer, String>(1, "b"),
                new Tuple2<Integer, String>(1, "c"),
                new Tuple2<Integer, String>(2, "d"),
                new Tuple2<Integer, String>(3, "e"));

        JavaPairRDD<Integer, String> javaPairRDD = sc.parallelizePairs(tuples);
        System.out.println(javaPairRDD.countByKey());

    }

}
