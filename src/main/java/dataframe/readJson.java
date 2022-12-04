package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class readJson {




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

    }
}
