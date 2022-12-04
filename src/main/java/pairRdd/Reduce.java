package pairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;


public class Reduce {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("appName").set("spark.driver.host","localhost").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		reduce(sc);
	}

	private static void reduce(JavaSparkContext sc) {
		
		List<Integer> numberList=Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> javaRDD = sc.parallelize(numberList);

		Integer num = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer num1, Integer num2) throws Exception {
				// System.out.println(num1+"======"+num2);
				return num1 + num2;
			}
		});
		Integer num2 = javaRDD.reduce((a,b)->a+b);
		System.out.println(num2+"------"+num);
		
		sc.close();
	}

}
