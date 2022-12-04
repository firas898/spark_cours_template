/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ML;

// $example on$

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off$

public class simple {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()      .master("local")
            .appName("JavaLinearRegressionWithElasticNetExample") .config("spark.driver.host", "localhost")
      .getOrCreate();

    // $example on$
    // Load training data.
    Dataset<Row> data = spark.read().option("inferSchema","true").option("header","true").csv("data/mllib/train.csv");

    data.printSchema();

    VectorAssembler assembler = new VectorAssembler()
            .setInputCols(new String[]{"ApplicantIncome","CoapplicantIncome"}).setOutputCol("features");

    Dataset<Row> output = assembler.setHandleInvalid("skip").transform(data);
output.toJavaRDD().collect().forEach(System.out::println);


  }
}
