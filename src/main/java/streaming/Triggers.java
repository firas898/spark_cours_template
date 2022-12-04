package streaming;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class Triggers {

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

		initDF=initDF.withColumn("Close",col("Close").cast(DoubleType)).withColumn("Open",col("Open").cast(DoubleType));
		Column ex = expr("IF((Close - Open) > 0, 'UP', 'DOWN')");

		UserDefinedFunction updown = udf(
				(Double close,Double open) -> {
					if ((open==null)||(close==null)) return "null";
					if ((close - open) > 0)
						return "UP";
					else
						return "Down";
				}, DataTypes.StringType);

		spark.udf().register("up_down", (Double close,Double open) -> {
			if ((open==null)||(close==null)) return "null";
			if ((close - open) > 0)
				return "UP";
			else
				return "Down";
		}, StringType);
		initDF.createOrReplaceTempView("initDF");

		Dataset<Row> resultDf = initDF.select(updown.apply(col("open"), col("close")));
		Dataset<Row> resultDf2 = spark.sql("select *, up_down(Close, Open) as up_down_udf from initDF");


		resultDf2.writeStream()
				.outputMode("append")
				.trigger(Trigger.ProcessingTime("1 second"))//Trigger.Once(), trigger(Trigger.ProcessingTime("1 minute"))
				.format("console")
				.option("truncate", false)
				.start()
				.awaitTermination();
	}


}
