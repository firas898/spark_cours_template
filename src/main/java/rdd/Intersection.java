package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;


public class Intersection {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("appName").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		intersection(sc);
	}

	static void intersection(JavaSparkContext sc) {
		List<String> datas1 = Arrays.asList("A", "B", "tom");
		List<String> datas2 = Arrays.asList("tom", "gim");


		JavaRDD<String> intersectionRDD = sc.parallelize(datas1).intersection(sc.parallelize(datas2));

		intersectionRDD.foreach(new VoidFunction<String>() {

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});

	}

}
