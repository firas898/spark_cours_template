package pairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class Join {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("appName").set("spark.driver.host","localhost").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		join(sc);
	}

	static void join(JavaSparkContext sc) {
	    List<Tuple2<Integer, String>> products = new ArrayList<>();
	    products.add(new Tuple2<>(1, "A"));
	    products.add(new Tuple2<>(2, "B"));
	    products.add(new Tuple2<>(3, "C"));
	    products.add(new Tuple2<>(4, "D"));

	    List<Tuple2<Integer, Integer>> counts = new ArrayList<>();
	    counts.add(new Tuple2<>(1, 7));
	    counts.add(new Tuple2<>(2, 3));
	    counts.add(new Tuple2<>(3, 8));
	    counts.add(new Tuple2<>(4, 3));
	    counts.add(new Tuple2<>(5, 9));

	    JavaPairRDD<Integer, String> productsRDD = sc.parallelizePairs(products);
	    JavaPairRDD<Integer, Integer> countsRDD = sc.parallelizePairs(counts);



	    productsRDD.join(countsRDD)
	            .foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
					@Override
					public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
						   System.out.println(t._1 + "\t" + t._2());
					}
				});
	}

}
