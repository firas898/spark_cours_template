package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class FlatMap {

	public static void main(String[] args) {
		//JavaSparkContext sc = SparkUtils.getLocalSparkContext(FlatMap.class);

		SparkConf conf = new SparkConf().setAppName("appName").set("spark.driver.host","localhost").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		flatMap(sc);
	}

	private static void flatMap(JavaSparkContext sc) {
		List<String> data = Arrays.asList("aa,bb,cc", "cxf,spring,struts2", "java,C++,javaScript");
		JavaRDD<String> rddData = sc.parallelize(data);

		FlatMapFunction<String, String> flatMapFunction=new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) throws Exception {
				List<String> list = Arrays.asList(s.split(","));
				return list.iterator();
			}
		};
		//JavaRDD<String> flatMapData = rddData.flatMap(flatMapFunction);
		JavaRDD<String> flatMapData = rddData.flatMap(x -> Arrays.asList(x.split(",")).iterator());
		flatMapData.foreach(new VoidFunction<String>() {
			@Override
			public void call(String v) throws Exception {
				System.out.println(v);
			}
		});

		sc.close();
	}
}
