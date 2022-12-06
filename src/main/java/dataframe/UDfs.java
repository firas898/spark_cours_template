package dataframe;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class UDfs {




    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("DataFrame-DatasetConversion").config("spark.driver.host", "localhost")
            .master("local[4]")
            .getOrCreate();

       // Dataset<Row> inputDf = spark.read().csv("data/dataframe/data2.csv");
       // inputDf.show(false);
        String usersCsvPath = "data/stream/AABA_2009.csv";






        StructType userDefinedSchema = new StructType()
                .add("Date", StringType, true)
                .add("Open", StringType, true)
                .add("High", StringType, true)
                .add("Low", StringType, true)
                .add("Close", StringType, true)
                .add("Volume", StringType, true)
                .add("name", StringType, true);



        Dataset<Row> usersDFShema = spark.read()
                .option("header", true)
                .schema(userDefinedSchema)
                .csv(usersCsvPath);
        usersDFShema.show();
        usersDFShema.show(false);


        Dataset<Row> initDF = usersDFShema.withColumn("Close", col("Close").cast(DoubleType)).withColumn("Open", col("Open").cast(DoubleType));
        Column ex = expr("IF((Close - Open) > 0, 'UP', 'DOWN')");

        UserDefinedFunction updown = udf(
                (Double close,Double open) -> {
                    if ((open==null)||(close==null)) return "null";
                    if ((close - open) > 0)
                        return "UP";
                    else
                        return "Down";
                }, DataTypes.StringType);

        spark.udf().register("up_down", (Double close,Double open) -> {
            if ((open==null)||(close==null)) return "null";
            if ((close - open) > 0)
                return "UP";
            else
                return "Down";
        }, StringType);
        initDF.createOrReplaceTempView("initDF");

        Dataset<Row> resultDf = initDF.select(updown.apply(col("open"), col("close")));
        Dataset<Row> resultDf2 = spark.sql("select *, up_down(Close, Open) as up_down_udf from initDF");
        resultDf2.show(false);



    }
}
