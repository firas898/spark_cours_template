package dataframe;


import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.functions.*;

public class dfdateTimes {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("RDD-Basic")
				.master("local[4]").config("spark.driver.host", "localhost")
				.getOrCreate();

		DataFrameReader sr = spark.read();

		String jsonpath="data/dataframe/data2.json";
		Dataset<Row> jsondf = spark.read().
				option("inferSchema", "true").json(jsonpath);
		jsondf.printSchema();
		jsondf.show();
		StructType userDefinedSchema = new StructType()
				.add("device", StringType, true)
				.add("event_name", StringType, true)
				.add("event_previous_timestamp", LongType, true)
				.add("event_timestamp", LongType, true)
				.add("geo", new StructType()
						.add("city", StringType, true)
						.add("state", StringType, true), true);


		Dataset<Row> newdf = jsondf.select(col("device"), col("event_timestamp").alias("timestamp"));
		newdf.show(false);
		Dataset<Row> castdf = newdf.select(col("device"), col("timestamp").divide(1000000).cast("timestamp").alias("ts2"),current_timestamp());
		castdf.show(false);

		castdf.select(col("device"),date_format(col("ts2"),"yyyy/MM/dd HH:mm:ss")).show();


		castdf.withColumn("year", year(col("ts2")))
				.withColumn("month", month(col("ts2")))
				.withColumn("minute", minute(col("ts2")))
				.withColumn("second", second(col("ts2"))).show(false);
		castdf.withColumn("date2", to_date(col("ts2"))).show(false);

		castdf.withColumn("date2", date_add(to_date(col("ts2")),2)).show(false);

	}


}
