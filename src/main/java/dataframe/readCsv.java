package dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class readCsv {




    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("DataFrame-DatasetConversion").config("spark.driver.host", "localhost")
            .master("local[4]")
            .getOrCreate();

       // Dataset<Row> inputDf = spark.read().csv("data/dataframe/data2.csv");
       // inputDf.show(false);
        String usersCsvPath = "data/dataframe/data2.csv";


        Dataset<Row> usersDF = spark.read()
                .option("sep", "\t")
                .option("header", true)
                .option("inferSchema", true)
                .csv(usersCsvPath);

        usersDF.printSchema();
        usersDF.show(false);



        StructType schema = new StructType(new StructField[]{
                new StructField("user_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("user_first_touch_timestamp", DataTypes.LongType, true, Metadata.empty()),
                new StructField("email", DataTypes.StringType, true, Metadata.empty()),

        });

        Dataset<Row> usersDFShema = spark.read()
                .option("sep", "\t")
                .option("header", true)
                .schema(schema)
                .csv(usersCsvPath);
        usersDFShema.show();
        usersDFShema.show(false);






    }
}
