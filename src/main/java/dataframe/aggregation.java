package dataframe;


import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static  org.apache.spark.sql.functions.*;

public class aggregation {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("RDD-Basic")
				.master("local[4]").config("spark.driver.host", "localhost")
				.getOrCreate();

		DataFrameReader sr = spark.read();

		Dataset<Row> realEstate = spark.read().option("inferschema","true").option("header", "true").csv("data/dataframe/RealEstate.csv");

		realEstate.groupBy("location").count().show();

		realEstate.groupBy("location").sum("price").show();

		realEstate.groupBy("location").agg(sum("price").alias("total sum"),count("*").as("totalcount")).show();

		realEstate.groupBy("location").agg(bround(avg("price"),2).alias("avg sum"),count("*").as("totalcount")).show();



}


}
