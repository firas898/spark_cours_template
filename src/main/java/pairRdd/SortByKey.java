package pairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SortByKey {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("appName").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sortByKey(sc);
	}

	private static void sortByKey(JavaSparkContext sc) {

		List<Integer> datas = Arrays.asList(60, 70, 80, 55, 45, 75);


		JavaRDD<Integer> sortByRDD = sc.parallelize(datas).sortBy(t->t, true, 1);

		sortByRDD.foreach(t->System.out.println(t));

		List<Tuple2<Integer, Integer>> datas2 = new ArrayList<>();
		datas2.add(new Tuple2<>(3, 3));
		datas2.add(new Tuple2<>(2, 2));
		datas2.add(new Tuple2<>(1, 4));
		datas2.add(new Tuple2<>(2, 3));


		JavaPairRDD<Integer, Integer> res2 = sc.parallelizePairs(datas2).sortByKey(false);
		res2.collect().forEach(v->System.out.println(v._1 + "==" + v._2));
	}

}
