package ML;

import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.SparkConf;

public class LinearRegressionMlib {

    public static void main(String[] args) {
        SparkConf configuration = new    SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost").setAppName("Linear  Regression");
                JavaSparkContext jsc = new
                        JavaSparkContext(configuration);

        // Load and parse the data
        String inputData = "data/mllib/ridge-data/lpsa.data";
        JavaRDD<String> data = jsc.textFile(inputData);

        data.take(5).forEach(System.out::println);

        JavaRDD<LabeledPoint> parsedData = data.map(line->
                {
                        String[] parts = line.split(",");
                       // System.out.println( "parts1    "+parts[1]);
                        String[] features = parts[1].split(" ");
                //    System.out.println( "parts1    "+features[1]);
                        double[] featureVector = new
                                double[features.length];
                        for (int i = 0; i < features.length - 1; i++){
                            featureVector[i] =
                                    Double.parseDouble(features[i]);
                        }
                        return new LabeledPoint(Double.parseDouble(parts[0]),
                                Vectors.dense(featureVector));

                }
        );

        parsedData.take(5).forEach(System.out::println);
        parsedData.cache();



        // Building the model
        int iterations = 1000;
        final LinearRegressionModel model =   LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData),  iterations);

        // Evaluate model on training examples and compute training error
        JavaRDD<Tuple2<Double, Double>> predictions = parsedData.map(point->{ double prediction = model.predict(point.features());
                    return new Tuple2<Double, Double>(prediction,
                            point.label());});

        double mse = new JavaDoubleRDD(predictions.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    private static final long serialVersionUID = 1L;

                    public Object call(Tuple2<Double, Double> pair) {
                        return Math.pow(pair._1() - pair._2(), 2.0);
                    }
                }
        ).rdd()).mean();
        System.out.println("training Mean Squared Error = " + mse);


        model.save(jsc.sc(), "target/tmp/javaLinearRegressionWithSGDModel");
        LinearRegressionModel sameModel = LinearRegressionModel.load(jsc.sc(),
                "target/tmp/javaLinearRegressionWithSGDModel");


        JavaRDD[] splits = data.randomSplit(new double[] {0.8, 0.2}, 11L);
        JavaRDD training = splits[0].cache();
        JavaRDD test = splits[1];


        jsc.close();

    }
}
