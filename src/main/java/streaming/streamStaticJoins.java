package streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.*;

import static org.apache.spark.sql.types.DataTypes.StringType;


public class streamStaticJoins {

	public static void main(String[] args) throws StreamingQueryException {
		//JavaSparkContext sc = SparkUtils.getLocalSparkContext(Filter.class);

		SparkSession spark = SparkSession
				.builder()
				.master("local")
				.appName("File Sink").config("spark.driver.host", "localhost")
				.getOrCreate();


		StructType userDefinedSchema = new StructType()
				.add("Date", StringType, true)
				.add("Open", StringType, true)
				.add("High", StringType, true)
				.add("Low", StringType, true)
				.add("Close", StringType, true)
				.add("Volume", StringType, true)
				.add("name", StringType, true);

		Dataset<Row> initDF = spark
				.readStream()
				.option("maxFilesPerTrigger", 2) // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
				.option("header", true)
				.schema(userDefinedSchema)
				.csv("data/stream");


		Dataset<Row> resultDf = initDF.select("Date", "Open", "Close");
		Dataset<Row> resultDF = initDF.groupBy(col("Name"), year(col("Date")).as("Year"))
				.agg(max("High").as("Max"));

		Dataset<Row> companyDF = spark.read().option("header", true)
				.csv("data/stocks/COMPANY.csv");

		companyDF.show();

		// Check if DataFrame is streaming or Not.
		System.out.println("resultDF Streaming DataFrame : " + resultDF.isStreaming());
		System.out.println("companyDF Streaming DataFrame : " + companyDF.isStreaming());
		// Static - Stream Joins
		// Inner join
//    val joinDf = resultDF.join(companyDF, Seq("Name"))

		// Left-outer Join : Stream - Static left outer join will work.
    /* Here we are matching all the records from Stream DataFrame on Left with Static DataFrame on Right.
    If records are not match from Stream DF (Left) to Static DF (Right) then NULL will be returned,
    since the data for Static DF will not change.
     */
//    val joinDf = resultDF.join(companyDF, Seq("Name"), "left_outer")

		// Left-outer Join : Static - Stream left outer join will not work.
    /* Here we are matching all the records from Static DataFrame on Left with Stream DataFrame on Right.
      If records are not match from Static DF (Left) to Stream DF (Right) then we cannot return NULL,
      since the Data is changing on Stream DF (Right) we cannot guarantee if we will get matching records or Not.
       */

		ArrayList<String> al = new ArrayList<String>();
		al.add("Name");

		Seq<String> scalaSeq = JavaConverters.asScalaIteratorConverter(al.iterator()).asScala().toSeq();

		Dataset<Row> joinDf = resultDF.join(companyDF,resultDF.col("Name"),"left_outer");

		Dataset<Row> joinDf2 = resultDF.join(companyDF,scalaSeq,"left_outer");


		joinDf.writeStream()
				.outputMode("append")
				.format("console")
				.option("truncate", false)
				.start()
				.awaitTermination();
	}


}
