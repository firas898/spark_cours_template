package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.functions.*;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class dateTimes {




    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("DataFrame-DatasetConversion")
            .master("local[4]")
            .getOrCreate();

       // Dataset<Row> inputDf = spark.read().csv("data/dataframe/data2.csv");
       // inputDf.show(false);
        String usersjsonPath = "data/dataframe/data2.json";


        Dataset<Row> eventsDF = spark.read()
                .option("inferSchema", true)
                .json(usersjsonPath);

        eventsDF.printSchema();

        StructType userDefinedSchema = new StructType()
                .add("device", StringType, true)
                .add("event_name", StringType, true)
                .add("event_previous_timestamp", LongType, true)
                .add("event_timestamp", StringType, true)
                .add("geo",  new StructType()
                        .add("city", StringType, true)
                        .add("state", StringType, true), true);

        Dataset<Row> eventsDFSchema = spark.read()
                .schema(userDefinedSchema)
                .json(usersjsonPath);
        eventsDFSchema.printSchema();
        eventsDFSchema.show(5,false);

        Dataset<Row> df = eventsDFSchema.select(col("device"), col("event_timestamp").alias("timestamp"));
//convert from microseconds to seconds
        Dataset<Row> df2 = df.withColumn("timestamp2", (col("timestamp").divide(1000000)).cast("timestamp"));

        df2 .show(false);
        df2.withColumn("date string", date_format(col("timestamp2"), "MMMM dd, yyyy"))
                .withColumn("time string", date_format(col("timestamp2"), "HH:mm:ss.SSSSSS")).show(false);

        df2.withColumn("year", year(col("timestamp2")))
                .withColumn("month", month(col("timestamp2")))
                .withColumn("minute", minute(col("timestamp2")))
                .withColumn("second", second(col("timestamp2"))).show(false);



        df2=df2 .withColumn("date", to_date(col("timestamp2")));
        df2.show(false);
        df2.withColumn("plus_two_days", date_add(col("date"), 2)).withColumn("date string2", date_add(col("date"), -2)).show(false);
    }
}
