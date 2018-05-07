package com.kenlu.crypto.analysis.unsupervised.kmeans;

import com.kenlu.crypto.analysis.factory.DataFactory;
import com.kenlu.crypto.analysis.formatter.DataFormatter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.TimerTask;

@Slf4j
@Component
public class KMeansClusteringAnalysis extends TimerTask {

    private static final int NUM_CLUSTERS = 12;
    private static final int NUM_ITERATIONS = 20;

    @Autowired
    private DataFactory dataFactory;
    @Autowired
    private DataFormatter dataFormatter;

    public void run() {
        JavaRDD<Vector> vectorJavaRDD =
                dataFormatter.toVectorJavaRDD(dataFactory.getPCADataset());

        KMeansModel clusters = KMeans.train(vectorJavaRDD.rdd(), NUM_CLUSTERS, NUM_ITERATIONS);

        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(vectorJavaRDD.rdd());
        System.out.println("Cost: " + cost);

// Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(vectorJavaRDD.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        System.out.println("--------------");
        clusters.predict(vectorJavaRDD).collect().stream().forEach(integer -> {
            System.out.print(integer);
            System.out.print(",");
        });
        System.out.println("--------------");

    }

}
