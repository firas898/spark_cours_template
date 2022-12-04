package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class Cartesian {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("appName").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		cartesian(sc);
	}

	private static void cartesian(JavaSparkContext sc) {
	    List<String> names = Arrays.asList("A", "V", "N");
	    List<Integer> scores = Arrays.asList(60, 70, 80);

	    JavaRDD<String> namesRDD = sc.parallelize(names);
	    JavaRDD<Integer> scoreRDD = sc.parallelize(scores);


	    JavaPairRDD<String, Integer> cartesianRDD = namesRDD.cartesian(scoreRDD);
	    
	    cartesianRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
	        public void call(Tuple2<String, Integer> t) throws Exception {
	            System.out.println(t._1 + "\t" + t._2());
	        }
	    });
	}
	
}
