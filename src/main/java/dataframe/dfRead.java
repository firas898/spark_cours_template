package dataframe;


import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;


public class dfRead {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("RDD-Basic")
				.master("local[4]").config("spark.driver.host", "localhost")
				.getOrCreate();

		DataFrameReader sr = spark.read();

		String csvpath="data/dataframe/data2.csv";

		Dataset<Row> csvdataset = spark.read()
				.option("sep","\t").option("inferSchema","true")
				.option("header","true").csv(csvpath);
		csvdataset.filter(col("user_id").isNull()).show(false);
		csvdataset.printSchema();
		csvdataset.show(false);



		StructType userDefinedSchema = new StructType()
				.add("user_id", StringType, true)
				.add("user_timestamp", LongType, true)
				.add("email", StringType, true);
		Dataset<Row> csvdatasetSchema = spark.read()
				.option("sep","\t").schema(userDefinedSchema)
				.option("header","true").csv(csvpath);

		csvdatasetSchema.printSchema();
		csvdatasetSchema.show(false);

	}


}
