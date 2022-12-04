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

import org.apache.spark.graphx.impl.*;

import org.apache.spark.graphx.lib.PageRank;

import org.apache.spark.graphx.util.*;
import org.apache.spark.storage.StorageLevel;
public class graph2 {

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

        listOfEdge.add(new Edge<>(1, 1, "Colleague"));
        listOfEdge.add(new Edge<>(4, 1, "Colleague"));
        listOfEdge.add(new Edge<>(5, 1, "Colleague"));
        JavaRDD<Tuple2<Object, String>> vertexRDD = javaSparkContext.parallelize(listOfVertex);
        JavaRDD<Edge<String>> edgeRDD = javaSparkContext.parallelize(listOfEdge);

        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

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


        Graph<String, String> graphreversed = graph.reverse();

        JavaRDD<String> resReversed = graphreversed.triplets().toJavaRDD().map(t -> t.srcAttr() + "is the " + t.attr() + " of " + t.dstAttr());
        resReversed.collect().forEach(System.out::println);

        //VertexRDD<Object> inDegrees = graph.ops().inDegrees().reduce((a,b)->if(a._2>b._2) a else b);
        VertexRDD<Tuple2<Object, String>[]> res2 = graph.ops().collectNeighbors(EdgeDirection.Either());
       // res2.toJavaRDD().collect().forEach(x->System.out.println(x._1+x._2));

      graph.ops().pageRank(5,0.00001).vertices().toJavaRDD().collect().forEach(p->System.out.println(p));





        javaSparkContext.close();
    }

}
