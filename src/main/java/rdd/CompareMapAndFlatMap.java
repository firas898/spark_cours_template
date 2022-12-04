package rdd;


import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Class CompareMapAndFlatMap. Example to compare Spark Map Transformation and
 * Spark FlatMap Transformation.
 *
 * @author neeraj
 *
 */
public class CompareMapAndFlatMap {

    private JavaSparkContext sparkContext = null;
    private String mapOut = "";



    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("appName").set("spark.driver.host","localhost").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
    CompareMapAndFlatMap compareMaps = new CompareMapAndFlatMap(sc);
		compareMaps.compare();}

    CompareMapAndFlatMap(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }


    public void compare() {
        /* Creating a List of Strings. */
        List<String> data = Arrays.asList("My Name Is Neeraj", "Hi Neeraj", "Happy New Year");

        /* Creating a JavaRDD of String from List data. */
        JavaRDD<String> lines = sparkContext.parallelize(data).cache();

        /* Printing the content in List data */
        System.out.println("List of String: " + data);

        /* Printing the records in JavaRDD lines */
        System.out.println(lines.collect());
        /*
         * Using Spark FlatMap Transformation and splitting each of the records
         * in JavaRDD lines.
         */
        JavaRDD<String> lineflatMap = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        /* Printing FlatMap Output */
        System.out.println("FlatMap Output: " + lineflatMap.collect());


        JavaRDD<String[]> lineMap = lines.map(s -> s.split(" "));


        lineMap.collect().forEach(x -> {
            mapOut = mapOut.concat("String Array: ");
            for (String i : x) {
                mapOut = mapOut.concat(i + " ");
            }
            mapOut = mapOut.concat("\n");
        });

        /* Printing Map Output */
        System.out.println("Map Output = \n" + mapOut);

    }

}