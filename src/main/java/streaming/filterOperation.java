package streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;
public class filterOperation {

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

		Dataset<Row> resultDF = initDF.select("Name", "Date", "Open", "Close")
				.filter((col("Close").cast("double").minus(col("Open").cast("double")) ).gt(0) );


		resultDF=initDF.select("Name", "Date", "High", "Close").groupBy(col("Name"), year(col("Date")).as("Year"))
				.agg(max("High").as("Max"));

		resultDF.writeStream()
				.outputMode("complete")
				.format("console")
				.option("truncate", false)
				.start()
				.awaitTermination();
	}


}
