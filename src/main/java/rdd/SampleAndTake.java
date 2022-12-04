package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;


public class SampleAndTake {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("appName").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sample(sc);
	}

	static void sample(JavaSparkContext sc) {
		List<Integer> datas = Arrays.asList(1, 2, 3, 7, 4, 5, 8);

		JavaRDD<Integer> dataRDD = sc.parallelize(datas);
		

		JavaRDD<Integer> sampleRDD = dataRDD.sample(false, 0.5, System.currentTimeMillis());
		
		// TODO dataRDD.takeSample(false, 3);
		// TODO dataRDD.take(3)

		sampleRDD.foreach(new VoidFunction<Integer>() {
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});

		sc.close();
	}

}
