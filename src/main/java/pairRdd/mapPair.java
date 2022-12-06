package pairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class mapPair {

	public static void main(String[] args) {
	SparkConf conf = new SparkConf().setAppName("appName2").set("spark.driver.host", "localhost").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);


		List<Tuple2<Integer, String>> products = new ArrayList<>();
		products.add(new Tuple2<>(1, "A"));
		products.add(new Tuple2<>(2, "B"));
		products.add(new Tuple2<>(3, "C"));
		products.add(new Tuple2<>(4, "D"));

		List<Tuple2<Integer, Integer>> counts = new ArrayList<>();
		counts.add(new Tuple2<>(1, 7));
		counts.add(new Tuple2<>(2, 3));
		counts.add(new Tuple2<>(3, 8));
		counts.add(new Tuple2<>(2, 3));
		counts.add(new Tuple2<>(1, 9));
		counts.add(new Tuple2<>(3, 9));
		JavaPairRDD<Integer, String> productsRdd = sc.parallelizePairs(products);
		JavaRDD<Tuple2<Integer, Integer>> countsRddt = sc.parallelize(counts);

		JavaPairRDD<Integer, Integer> countsRdd = JavaPairRDD.fromJavaRDD(countsRddt);

		productsRdd.join(countsRdd).collect().forEach(System.out::println);

		JavaPairRDD<Integer, Iterable<Integer>> res = countsRdd.groupByKey();
		res.collect().forEach(System.out::println);

		Map<Integer, Long> resCount = countsRdd.countByKey();
		System.out.println(resCount);
		JavaPairRDD<Integer, Integer> resReduce = countsRdd.reduceByKey((a, b) -> a + b);

	System.out.println("Reduce By Key");
		resReduce.collect().forEach(System.out::println);

		System.out.println("Sort By Key");
		JavaPairRDD<Integer, Integer> resSort = countsRdd.sortByKey(false);

		resSort.collect().forEach(System.out::println);


		List<Tuple2<Integer, Integer>> list2 = sc.parallelizePairs(counts, 2)
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				}).collect();
	}


}
