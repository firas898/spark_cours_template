package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class MapPartitions {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("appName").set("spark.driver.host","localhost").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		mapPartitions(sc);
	}

	private static void mapPartitions(JavaSparkContext sc) {
		List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J");

		JavaRDD<String> namesRDD = sc.parallelize(names, 3);
		JavaRDD<String> mapPartitionsRDD = namesRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			int count = 0;

			@Override
			public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
				List<String> list = new ArrayList<String>();
				while (stringIterator.hasNext()) {
					list.add("count:" + count++ + "\t" + stringIterator.next());
				}
				return list.iterator();
			}
		});

		List<String> result = mapPartitionsRDD.collect();
		for (String s : result) {
			System.out.println(s);
		}

		sc.close();
	}

}
