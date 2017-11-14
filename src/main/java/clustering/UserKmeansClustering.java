package clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.*;
import java.util.Collection;

// $example off$

public class UserKmeansClustering {

    public void UserClustering(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UserKmeansClustering");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String feature_path = "data\\feature.txt";
        JavaRDD<String> data = jsc.textFile(feature_path);

        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            int featureNum = sarray.length;
            if (featureNum != 9) {
                System.out.println("error feature num: " + s);
            }
            double[] values = new double[featureNum];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Integer.parseInt(sarray[i]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();

        // Cluster the data into three classes using KMeans
        int numClusters = 3;
        int numIterations = 100;

        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        Collection<Integer> predictResult = parsedData.map(v -> clusters.predict(v)).collect();

        ResultAnalysis.saveResult(predictResult, "kmeans");

        jsc.stop();

    }

//    private <T> void saveResult(Collection<T> predictResult){
//        String cluster_0_path = "data\\result\\kmeans_cluster_0.txt";
//        String cluster_1_path = "data\\result\\kmeans_cluster_1.txt";
//        String cluster_2_path = "data\\result\\kmeans_cluster_2.txt";
//        String feature_path = "data\\feature.txt";
//        File featureFile = new File(feature_path);
//        File cluster_0 = new File(cluster_0_path);
//        File cluster_1 = new File(cluster_1_path);
//        File cluster_2 = new File(cluster_2_path);
//
//        try {
//            cluster_0.createNewFile();
//            cluster_1.createNewFile();
//            cluster_2.createNewFile();
//
//            BufferedWriter cluster_0_writer = new BufferedWriter(new FileWriter(cluster_0));
//            BufferedWriter cluster_1_writer = new BufferedWriter(new FileWriter(cluster_1));
//            BufferedWriter cluster_2_writer = new BufferedWriter(new FileWriter(cluster_2));
//
//            BufferedReader featureReader = new BufferedReader(new FileReader(featureFile));
//            String featureLine = "";
//
//            for (T result : predictResult) {
//                featureLine = featureReader.readLine();
//                if (featureLine == null) {
//                    System.out.println("null feature line");
//                    break;
//                }
//                if ((Integer)result == 0) {
//                    cluster_0_writer.write(featureLine + "\r\n");
//                } else if ((Integer)result == 1) {
//                    cluster_1_writer.write(featureLine + "\r\n");
//                } else if ((Integer)result == 2) {
//                    cluster_2_writer.write(featureLine + "\r\n");
//                } else {
//                    System.out.println("error class predict");
//                }
//            }
//
//            cluster_0_writer.flush();
//            cluster_1_writer.flush();
//            cluster_2_writer.flush();
//
//            cluster_0_writer.close();
//            cluster_1_writer.close();
//            cluster_2_writer.close();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    public static void main(String[] args) {

        UserKmeansClustering ukc = new UserKmeansClustering();
        ukc.UserClustering();

    }

}
