package graphx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.List;

public class graph4 {

    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext javaSparkContext = new JavaSparkContext(
                new SparkConf().setMaster("local").set("spark.driver.host", "localhost").setAppName("EdgeCount"));



        List<Tuple2<Object, String>> listOfVertex = new ArrayList<>();
        listOfVertex.add(new Tuple2<>(1l, "James"));
        listOfVertex.add(new Tuple2<>(2l, "Andy"));
        listOfVertex.add(new Tuple2<>(3l, "Ed"));
        listOfVertex.add(new Tuple2<>(4l, "Roger"));
        listOfVertex.add(new Tuple2<>(5l, "Tony"));

        List<Edge<String>> listOfEdge = new ArrayList<>();
        listOfEdge.add(new Edge<>(2, 1, "Friend"));
        listOfEdge.add(new Edge<>(3, 1, "Friend"));
        listOfEdge.add(new Edge<>(3, 2, "Colleague"));
        listOfEdge.add(new Edge<>(3, 5, "Partner"));
        listOfEdge.add(new Edge<>(4, 3, "Boss"));
        listOfEdge.add(new Edge<>(5, 2, "Partner"));

        JavaRDD<Tuple2<Object, String>> vertexRDD = javaSparkContext.parallelize(listOfVertex);
        JavaRDD<Edge<String>> edgeRDD = javaSparkContext.parallelize(listOfEdge);

        ClassTag<String> stringTag = ClassTag$.MODULE$.apply(String.class);

        Graph<String, String> graph = Graph.apply(
                vertexRDD.rdd(),
                edgeRDD.rdd(),
                "",
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                stringTag,
                stringTag
        );

        //apply specific algorithms, such as PageRank

        String VERTICES_FOLDER_PATH =
                "D:\\streaming\\AnalysisTier\\graphx\\vertices";
        String EDGES_FOLDER_PATH =
                "D:\\streaming\\AnalysisTier\\graphx\\edges";

       // graph.vertices().saveAsTextFile(VERTICES_FOLDER_PATH);
        graph.vertices().toJavaRDD().collect().forEach(System.out::println);
        graph.edges().toJavaRDD().collect().forEach(System.out::println);
        JavaRDD<String> res = graph.triplets().toJavaRDD().map(t -> t.srcAttr() + "is the " + t.attr() + " of " + t.dstAttr());
        res.collect().forEach(System.out::println);
        //graph.edges().saveAsTextFile(EDGES_FOLDER_PATH);

        graph.vertices().toJavaRDD().filter(a->a._2=="5");



        javaSparkContext.close();
    }

}
