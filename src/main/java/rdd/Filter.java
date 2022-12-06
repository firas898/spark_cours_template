package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Filter {

	public static void main(String[] args) {
		//JavaSparkContext sc = SparkUtils.getLocalSparkContext(Filter.class);

		List<Integer> datas = Arrays.asList(1, 2, 3, 7, 4, 5, 8);
		List<String> dataString = Arrays.asList("aa,bb,cc", "cxf,spring,struts2", "java,C++,javaScript");
		SparkConf conf = new SparkConf().setAppName("appName2").set("spark.driver.host", "localhost").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);





		JavaRDD<Integer> rddData = sc.parallelize(datas);
		JavaRDD<Integer> filterRDD = rddData.filter(v1 -> v1 >= 3);

		JavaRDD<String> dataStringRdd=sc.parallelize(dataString);
		//dataStringRdd.collect().forEach(System.out::println);

		JavaRDD<String> dataFlatMap = dataStringRdd.flatMap(ligne -> Arrays.asList(ligne.split(",")).iterator());

		JavaRDD<String[]> MapData = dataStringRdd.map(x -> x.split(","));

		dataFlatMap.foreach(v -> System.out.println(v));
		//MapData.foreach(v -> System.out.println(v));
		System.out.println(dataFlatMap.count());
		//System.out.println(MapData.count());



	}


}
