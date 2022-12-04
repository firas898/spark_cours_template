package streaming;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.spark_project.guava.collect.ImmutableMap;

import static org.apache.spark.sql.types.DataTypes.StringType;


public class forEachBatch {

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



	/*	query = spark
				.readStream()
				.textFile(input)
				.writeStream()
				.foreach(new ForeachWriter<String>() {
					@Override
					public boolean open(long partitionId, long epochId) {
						return true;
					}

					@Override
					public void process(String value) {
					}

					@Override
					public void close(Throwable errorOrNull) {
					}
				})
				.start();

		query2 = initDF.writeStream()
				.foreach(new ForeachWriter<String>() {
					@Override
					public boolean open(long partitionId, long epochId) {
						return true;
					}

					@Override
					public void process(String value) {
					}

					@Override
					public void close(Throwable errorOrNull) {
					}

				}).start().awaitTermination();
*/
	}


}
