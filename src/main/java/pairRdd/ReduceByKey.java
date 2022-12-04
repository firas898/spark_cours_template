package pairRdd;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class ReduceByKey {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("appName").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		reduceByKey(sc);
	}


	private static void reduceByKey(JavaSparkContext sc) {

		JavaRDD<String> lines = sc.textFile("src/main/java/dataframe/README.md");


		JavaRDD<String> wordsRDD = lines.flatMap(line-> Arrays.asList(line.split(" ")).iterator());



		JavaPairRDD<String, Integer> wordsCount = wordsRDD.mapToPair(word->new Tuple2<String, Integer>(word, 1));

		JavaPairRDD<String, Integer> resultRDD = wordsCount.reduceByKey((a,b)->a+b);

		resultRDD.foreach(t->System.out.println(t._1 + "\t" + t._2()));

		resultRDD.saveAsTextFile("data/resources/wordcount_result");

		sc.close();
	}

}
