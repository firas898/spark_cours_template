package dataframe;


import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.JoinType;

import static org.apache.spark.sql.functions.*;

public class Joins {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("RDD-Basic")
				.master("local[4]").config("spark.driver.host", "localhost")
				.getOrCreate();

		DataFrameReader sr = spark.read();

		Dataset<Row> empDF = spark.read().option("inferschema","true").option("header", "true").csv("data/dataframe/employee.csv");
		Dataset<Row> deptDF = spark.read().option("inferschema","true").option("header", "true").csv("data/dataframe/dept.csv");
		empDF.show();
		deptDF.show();
		empDF=empDF.filter(col("name").isNotNull());
		Dataset<Row> df2 = empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(col("dept_id")), "inner");

	/*	empDF.join(deptDF).where(col("emp_dept_id").equalTo(col("dept_id")));
		empDF.createOrReplaceTempView("empt");
		deptDF.createOrReplaceTempView("deptt");

		spark.sql("select * from empt, deptt where emp_dept_id==dept_id");
		spark.sql("select * from empt inner join deptt on emp_dept_id==dept_id");

		empDF.join(broadcast(deptDF),empDF.col("emp_dept_id").equalTo(col("dept_id")),"full").show();
		empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(col("dept_id")),"leftsemi").show();
		empDF.join(deptDF,empDF.col("emp_dept_id").equalTo(col("dept_id")),"leftanti").show();


		Dataset<Row> selfDf = empDF.as("emp1").join(empDF.as("emp2"),
				col("emp1.superior_emp_id").equalTo(col("emp2.emp_id")), "inner");
		selfDf.select(col("emp1.emp_id"),col("emp1.name"),
						col("emp2.emp_id").as("superior_emp_id"),
						col("emp2.name").as("superior_emp_name"))
				.show(false);
*/
	df2.show();
		df2.explain();

	}


}
