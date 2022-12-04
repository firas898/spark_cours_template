package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.functions.*;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class join {




    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("DataFrame-DatasetConversion").config("spark.driver.host", "localhost")
            .master("local[4]")
            .getOrCreate();

       // Dataset<Row> inputDf = spark.read().csv("data/dataframe/data2.csv");
       // inputDf.show(false);
        String empCsvPath = "data/dataframe/employee.csv";

        String deptCsvPath = "data/dataframe/dept.csv";

        StructType empSchema = new StructType()
                .add("emp_id", StringType, true)
                .add("name", StringType, true)
                .add("superior_emp_id", LongType, true)
                .add("year_joined", StringType, true)
                .add("emp_dept_id", IntegerType, true)
                 .add("gender", StringType, true)
                 .add("salary", LongType, true);
        StructType deptSchema = new StructType()
                .add("dept_name", StringType, true)
                .add("dept_id", IntegerType, true);

        Dataset<Row> empDF = spark.read()
                .option("header", true)
                .schema(empSchema)
                .csv(empCsvPath);
        Dataset<Row> deptDF = spark.read()
                .option("header", true)
                .schema(deptSchema)
                .csv(deptCsvPath);
        empDF.show();
        deptDF.show(false);



        //innerjoin
        System.out.println("innerjoin");
        empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")),"inner").show(false);
        System.out.println("innerjoin  Where");
        empDF.join(deptDF).where(empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")))
                .show(false);


        empDF.createOrReplaceTempView("EMP");
        deptDF.createOrReplaceTempView("DEPT");

        //SQL JOIN
        System.out.println("innerjoin  SQL join");
         spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id").show(false);

         spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id").show(false);


        // Full Outer Join
        empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")),"outer")
                .show(false);
        empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")),"full")
                .show(false);
        empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")),"fullouter")
                .show(false);


        //Left Outer Join


        empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")),"left")
                .show(false);
        empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")),"leftouter")
                .show(false);
        //Right Outer Join

        empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")),"right")
                .show(false);
        empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")),"rightouter")
                .show(false);
        //  Left Semi Join

        empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")),"leftsemi")
                .show(false);

        //Left Anti Join
        empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")),"leftanti")
                .show(false);


        //self join
        Dataset<Row> selfDf = empDF.as("emp1").join(empDF.as("emp2"),
                col("emp1.superior_emp_id").equalTo(col("emp2.emp_id")), "inner");
        selfDf.select(col("emp1.emp_id"),col("emp1.name"),
                        col("emp2.emp_id").as("superior_emp_id"),
                        col("emp2.name").as("superior_emp_name"))
                .show(false);


    }
}
