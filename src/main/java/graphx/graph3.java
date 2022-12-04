package graphx;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;


public class graph3 {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(
                new SparkConf().setMaster("local").set("spark.driver.host", "localhost").setAppName("EdgeCount"));


        ClassTag<String> stringTag = ClassTag$.MODULE$.apply(String.class);


        List<Edge<String>> edges = new ArrayList<>();

        edges.add(new Edge<String>(1, 2, "Friend"));
        edges.add(new Edge<String>(2, 3, "Advisor"));
        edges.add(new Edge<String>(1, 3, "Friend"));
        edges.add(new Edge<String>(4, 3, "colleague"));
        edges.add(new Edge<String>(4, 5, "Relative"));
        edges.add(new Edge<String>(2, 5, "BusinessPartners"));


        JavaRDD<Edge<String>> edgeRDD = javaSparkContext.parallelize(edges);


        Graph<String, String> graph = Graph.fromEdges(edgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);


        graph.vertices().toJavaRDD().collect().forEach(System.out::println);

        graph.edges().toJavaRDD().collect().forEach(System.out::println);
        VertexRDD<String> xd = graph.vertices();
        JavaRDD<Tuple2<Object, String>> x = graph.vertices().toJavaRDD();
        graph.triplets().toJavaRDD().collect().forEach(System.out::println);

        //VertexRDD<String> filtergraph = graph.vertices().filter(a -> a._2 == "5");

        graph.edges().toJavaRDD().filter(a->a.srcId()>a.dstId());
        graph.triplets().toJavaRDD().collect().forEach(System.out::println);
        JavaRDD<String> res = graph.triplets().toJavaRDD().map(t -> t.srcAttr() + "is the " + t.attr() + "of" + t.dstAttr());
        res.collect().forEach(System.out::println);

        //RDD<String> xx = graph.triplets().map(t -> t.srcAttr().toString(),stringTag);
        //  graph.edges().toJavaRDD().filter(case
           //     graph.vertices().toJavaRDD().filter ( case (id, name) => id > 30 )
        //graph.vertices().filter();
//	graph.aggregateMessages(sendMsg, mergeMsg, tripletFields, evidence$11)

    }
}
