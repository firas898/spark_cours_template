package streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;


public class kafkaSource {

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
				.format("kafka")
				.option("kafka.bootstrap.servers", "PORT_9XFTJC2.UMANIS.com:9092")
				.option("subscribe", "mft")
				.option("startingOffsets","earliest").load();


		Dataset<Row> resultDf = initDF.select("Date", "Open", "Close");

		resultDf.writeStream()
				.outputMode("append")
				.format("console")
				.option("truncate", false)
				.start()
				.awaitTermination();
	}


}
