package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;


public class intersection {

	public static void main(String[] args) {
		//JavaSparkContext sc = SparkUtils.getLocalSparkContext(Filter.class);

			SparkConf conf = new SparkConf().setAppName("appName2").set("spark.driver.host", "localhost").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);


		List<String> datas1 = Arrays.asList("A", "B", "tom","ddd","erer");
		List<String> datas2 = Arrays.asList("tom", "gim");

		JavaRDD<String> rddp = sc.parallelize(datas1, 4);
		JavaRDD<String> lines = sc.textFile("README.md");
		lines.take(3).forEach(System.out::println);


		System.out.println("RDD: " + rddp.partitions().size());

		JavaRDD<String> rdd2 = rddp.coalesce(5);


		Broadcast<List<String>>broadcastVar = sc.broadcast(datas1);

		LongAccumulator accum = sc.sc().longAccumulator();

		sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));

		accum.value();
		System.out.println("RDD: " + rdd2.partitions().size());
		System.out.println(accum.value());
	}


}
