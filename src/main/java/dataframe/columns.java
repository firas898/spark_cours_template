package dataframe;


import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class columns {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("RDD-Basic")
				.master("local[4]").config("spark.driver.host", "localhost")
				.getOrCreate();


		String csvpath="data/dataframe/RealEstate.csv";

		Dataset<Row> realEstate = spark.read()
				.option("inferSchema","true")
				.option("header","true").csv(csvpath);
	realEstate.printSchema();
		realEstate.show();
		realEstate.filter("Bedrooms>2");
		realEstate.filter(col("bedrooms").gt(2)).show();


		realEstate.filter(col("Price SQ Ft").gt(200)).show();

		realEstate=	realEstate.withColumnRenamed("Price SQ Ft","Price_SQ_Ft");

		realEstate.show(false);

		realEstate.withColumn("Petite",col("Bedrooms").isin(1,2)).show(false);

		Dataset<Row> c1 = realEstate.filter("Bedrooms is not null").filter(col("Bedrooms").gt(2));
		Dataset<Row> c2= realEstate.filter((col("Bedrooms").isNotNull()).and (col("Bedrooms").gt(2)) ) ;
		Dataset<Row> c3= realEstate.filter("Bedrooms is not null and Bedrooms>2 ");

		System.out.println(c1.count()+"**"+c2.count()+"**"+c3.count()+"");
		realEstate.printSchema();
		realEstate.withColumn("price", col("price").cast("long")).withColumn("size", col("size").cast("long")).orderBy("price","size").show();

		realEstate.orderBy("price","size").show(100,false);
	}


}
