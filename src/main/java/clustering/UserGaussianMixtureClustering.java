package clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.GaussianMixture;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Collection;

public class UserGaussianMixtureClustering {

    public void userClustering(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UserKmeansClustering");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String feature_path = "data\\feature.txt";
        JavaRDD<String> data = jsc.textFile(feature_path);

        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            int featureNum = sarray.length;
            double[] values = new double[featureNum];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Integer.parseInt(sarray[i]);
            }
            return Vectors.dense(values);
        });

        parsedData.cache();

        // Cluster the data into three classes using GaussianMixture
        GaussianMixtureModel gmm = new GaussianMixture().setK(3).run(parsedData.rdd());

        Collection<Integer> predictResult = parsedData.map(v -> gmm.predict(v)).collect();

        ResultAnalysis.saveResult(predictResult, "gaussian");

    }

    public static void main(String[] args) {
        UserGaussianMixtureClustering ugmc = new UserGaussianMixtureClustering();
        ugmc.userClustering();
    }

}
