package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.callUDF;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class write {




    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("DataFrame-DatasetConversion")
            .master("local[4]")
            .getOrCreate();

       // Dataset<Row> inputDf = spark.read().csv("data/dataframe/data2.csv");
       // inputDf.show(false);
        String usersCsvPath = "data/dataframe/data2.csv";


        Dataset<Row> eventsDF = spark.read()
                .option("sep", "\t")
                .option("header", true)
                .option("inferSchema", true)
                .csv(usersCsvPath);

        eventsDF.printSchema();


        eventsDF.write().format("json").mode("overwrite").save("data/outjson");
        eventsDF.write().format("csv").mode("overwrite").save("data/outcsv");

        String outputSnappy = "output/Snappy/";

        eventsDF.coalesce(2).write()
         .option("compression", "snappy")
         .mode("overwrite")
         .parquet(outputSnappy);


        Dataset<Row> eventsDFpar = spark.read()
                .format("parquet")
                .load(outputSnappy);

        eventsDF.write().mode("overwrite").saveAsTable("events_s");

    }
}
