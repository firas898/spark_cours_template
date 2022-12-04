package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;


public class Coalesce {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("appName").setMaster("local[4]");//.config("spark.driver.host", "localhost");
		JavaSparkContext sc = new JavaSparkContext(conf);

		coalesce(sc);
	}

	private static void coalesce(JavaSparkContext sc) {
		List<String> datas = Arrays.asList("hi", "hello", "how", "are", "you");
		JavaRDD<String> datasRDD = sc.parallelize(datas, 4);
		System.out.println("RDD: " + datasRDD.partitions().size());
		JavaRDD<String> datasRDD2 = datasRDD.coalesce(2, false);
		System.out.println("RDD: " + datasRDD2.partitions().size());
	}

}
