package streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.VoidFunction2;
import org.spark_project.guava.collect.ImmutableMap;

import static org.apache.spark.sql.types.DataTypes.StringType;


public class forEachBatchSink {

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
		/*StreamingQuery query = initDF.writeStream()
				.outputMode("update")
				.option("checkpointLocation", "webhdfs://127.0.0.1:5598/checkpoint")
				.foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, batchId) ->
						df.write()
								.format("org.apache.spark.sql.cassandra")
								.options(ImmutableMap.of("table", "sttest", "keyspace", "test"))
								.mode("append")
								.save()
				)
				.start();

		query.awaitTermination();*/
	}


}
