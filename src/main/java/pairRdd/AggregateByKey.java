package pairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

class AggregateByKey {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("appName").setMaster("local[4]");//.config("spark.driver.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);

        aggregateByKey(sc);
    }

    private static void aggregateByKey(JavaSparkContext sc) {
        List<Tuple2<Integer, Integer>> datas = new ArrayList<>();
        datas.add(new Tuple2<>(1, 3));
        datas.add(new Tuple2<>(1, 2));
        datas.add(new Tuple2<>(1, 4));
        datas.add(new Tuple2<>(2, 3));

        List<Tuple2<Integer, Integer>> list = sc.parallelizePairs(datas, 2)
                .aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        System.out.println("seq: " + v1 + "\t" + v2);
                        return v1 + v2;
                    }
                }, new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        System.out.println("comb: " + v1 + "\t" + v2);
                        return v1 + v2;
                    }
                }).collect();



        List<Tuple2<Integer, Integer>> list2 = sc.parallelizePairs(datas, 2)
                .reduceByKey(new Function2<Integer, Integer, Integer>() {

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }).collect();

        for (Tuple2 t : list) {
            System.out.println(t._1 + "=====" + t._2);
        }

        for (Tuple2 t : list2) {
            System.out.println(t._1 + "=====" + t._2);
        }
    }

}
