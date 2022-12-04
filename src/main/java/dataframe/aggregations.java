package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class aggregations {




    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("DataFrame-DatasetConversion")
            .master("local[4]")
            .getOrCreate();

        Dataset<Row> realEstate = spark.read().option("header", "true").csv("data/dataframe/RealEstate.csv");

        realEstate.printSchema();


        realEstate.show();


        //nombre des lignes par location
        realEstate.groupBy("Location").count().show();

        //moyen prix per location
        Dataset<Row> realEstateT = realEstate.withColumn("price", col("price").cast("long"));
        realEstateT.groupBy("Location").avg("Price").show();
//total price par location et Status
        realEstateT.groupBy("Location","Status").sum("Price").show();


        realEstateT.groupBy("Location").agg(sum(col("Price")).alias("Total"),bround(avg(col("Price")).alias("moyen"),2)).show();


        realEstateT.groupBy("Location").agg(max(col("price")),min(col("price")),mean(col("price"))).show();
    }
}
