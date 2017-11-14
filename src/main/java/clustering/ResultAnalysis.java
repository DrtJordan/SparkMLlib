package clustering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import java.io.*;
import java.util.Collection;


public class ResultAnalysis {

    public static <T> void saveResult(Collection<T> predictResult, String clfName){

        System.out.println("result of " + clfName);

        String cluster_0_path = "data\\result\\" + clfName + "_cluster_0.txt";
        String cluster_1_path = "data\\result\\" + clfName + "_cluster_1.txt";
        String cluster_2_path = "data\\result\\" + clfName + "_cluster_2.txt";
        String feature_path = "data\\feature.txt";
        File featureFile = new File(feature_path);
        File cluster_0 = new File(cluster_0_path);
        File cluster_1 = new File(cluster_1_path);
        File cluster_2 = new File(cluster_2_path);

        try {
            cluster_0.createNewFile();
            cluster_1.createNewFile();
            cluster_2.createNewFile();

            BufferedWriter cluster_0_writer = new BufferedWriter(new FileWriter(cluster_0));
            BufferedWriter cluster_1_writer = new BufferedWriter(new FileWriter(cluster_1));
            BufferedWriter cluster_2_writer = new BufferedWriter(new FileWriter(cluster_2));

            BufferedReader featureReader = new BufferedReader(new FileReader(featureFile));
            String featureLine = "";

            for (T result : predictResult) {
                featureLine = featureReader.readLine();
                if (featureLine == null) {
                    System.out.println("null feature line");
                    break;
                }
                if ((Integer)result == 0) {
                    cluster_0_writer.write(featureLine + "\r\n");
                } else if ((Integer)result == 1) {
                    cluster_1_writer.write(featureLine + "\r\n");
                } else if ((Integer)result == 2) {
                    cluster_2_writer.write(featureLine + "\r\n");
                } else {
                    System.out.println("error class predict");
                }
            }
            cluster_0_writer.flush();
            cluster_1_writer.flush();
            cluster_2_writer.flush();

            cluster_0_writer.close();
            cluster_1_writer.close();
            cluster_2_writer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }

}
