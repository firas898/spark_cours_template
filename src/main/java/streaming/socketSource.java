package streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class socketSource {

	public static void main(String[] args) throws StreamingQueryException {
		//JavaSparkContext sc = SparkUtils.getLocalSparkContext(Filter.class);

		SparkSession spark =SparkSession
				.builder()
				.master("local")
				.appName("Rate Source").config("spark.driver.host", "localhost")
				.getOrCreate();



		StructType userDefinedSchema = new StructType()
				.add("Date", StringType, true)
				.add("Open", StringType, true)
				.add("High", StringType, true)
				.add("Low", StringType, true)
				.add("Close", StringType, true)
				.add("Volume", StringType, true)
				.add("name", StringType, true);
		String host = "127.0.0.1";
		String port = "9999";
		Dataset<Row> initDF =spark
				.readStream()
				.format("socket")
				.option("host", host)
				.option("port", port)
				.load();


		Dataset<Row> resultDF = initDF
				.withColumn("result",concat( col("value"),lit(1)));

		resultDF.writeStream()
				.outputMode("append")
				.format("console")
				.option("truncate", false)
				.start()
				.awaitTermination();
	}


}
