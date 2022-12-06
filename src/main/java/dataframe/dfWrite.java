package dataframe;


import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class dfWrite {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("RDD-Basic")
				.master("local[4]").config("spark.driver.host", "localhost")
				.getOrCreate();

		DataFrameReader sr = spark.read();

		String csvpath="data/dataframe/RealEstate.csv";

		Dataset<Row> csvdataset = spark.read()
				.option("inferSchema","true")
				.option("header","true").csv(csvpath);
		csvdataset.printSchema();
		csvdataset.show(false);
		csvdataset=csvdataset.orderBy("price");
		//csvdataset.repartition(4,col("location")).write().format("csv").mode("overwrite").save("data/outputcsv");
		//csvdataset.coalesce(1).write().format("json").mode("overwrite").save("data/outputjson");
		csvdataset=csvdataset.withColumnRenamed("Price SQ Ft","PriceSQ");
csvdataset.write().option("compression","snappy").mode("overwrite").save("output/snappy");
		spark.read().format("parquet").load("output/snappy").show(false);
	}


}
