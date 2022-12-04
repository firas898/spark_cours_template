package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;


public class Filter {

	public static void main(String[] args) {
		//JavaSparkContext sc = SparkUtils.getLocalSparkContext(Filter.class);

		List<Integer> datas = Arrays.asList(1, 2, 3, 7, 4, 5, 8);

		SparkConf conf = new SparkConf().setAppName("appName2").setMaster("local[4]");//.config("spark.driver.host", "localhost");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> rddData = sc.parallelize(datas);
		JavaRDD<Integer> filterRDD = rddData.filter(v1 -> v1 >= 3);


		filterRDD.foreach(v -> System.out.println(v));

	}


}
