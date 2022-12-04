package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;


public class Union {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("appName").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		union(sc);
	}

	static void union(JavaSparkContext sc ) {
	    List<String> datas1 = Arrays.asList("AAA", "BBB");
	    List<String> datas2 = Arrays.asList("tom", "gim");

	    JavaRDD<String> data1RDD = sc.parallelize(datas1);
	    JavaRDD<String> data2RDD = sc.parallelize(datas2);


	    JavaRDD<String> unionRDD = data1RDD
	            .union(data2RDD);

	    unionRDD.foreach(new VoidFunction<String>() {
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});

	    sc.close();
	}

}
