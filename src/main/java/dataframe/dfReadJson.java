package dataframe;


import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class dfReadJson {

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


		Dataset<Row> jsondf2 = spark.read().
				schema(userDefinedSchema).json(jsonpath);
		jsondf2.printSchema();
		jsondf2.show();
	}


}
