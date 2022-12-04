package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class culomns {




    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("DataFrame-DatasetConversion")
            .master("local[4]")
            .getOrCreate();

        Dataset<Row> realEstate = spark.read().option("header", "true").csv("data/dataframe/RealEstate.csv");

        realEstate.printSchema();


        realEstate.show();

        realEstate.filter("Bedrooms is not null").filter(col("Bedrooms").gt(2) ) ;
        realEstate.filter("Bedrooms >2") ;

        realEstate.select("Location","price").show();


        realEstate.select(col("Location"),col("price").alias("price en dollar")).show();
        Dataset<Row> realEstate2 = realEstate.withColumnRenamed("price sq ft", "price_sq_ft");
        realEstate2.selectExpr("location"," price_sq_ft > 200  as chere ").show();

        realEstate.drop("MLS").show();



        realEstate.withColumn("petit",col("Bedrooms").isin (1,2)).show();

        Dataset<Row> c1 = realEstate.filter("Bedrooms is not null").filter(col("Bedrooms").gt(2));
        Dataset<Row> c2= realEstate.filter((col("Bedrooms").isNotNull()).and (col("Bedrooms").gt(2)) ) ;
        Dataset<Row> c3= realEstate.filter("Bedrooms is not null and Bedrooms>2 ");

        realEstate.filter("Bedrooms >2") ;

System.out.println(c1.count()+"**"+c2.count()+"**"+c3.count()+"");

        realEstate.dropDuplicates("Location").show();


        realEstate.sort("price").show();
        realEstate.sort(col("price").desc()).show();


        realEstate.orderBy("price","size").show();

        realEstate.withColumn("price", col("price").cast("long")).withColumn("size", col("size").cast("long")).orderBy("price","size").show();

    }
}
