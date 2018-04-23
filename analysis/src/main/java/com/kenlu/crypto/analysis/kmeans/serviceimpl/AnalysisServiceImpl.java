package com.kenlu.crypto.analysis.kmeans.serviceimpl;

import com.kenlu.crypto.domain.Crypto;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Slf4j
@Component
public class AnalysisServiceImpl implements CommandLineRunner {

    @Autowired
    private DataFactory dataFactory;

    @Override
    public void run(String... args) {

        // TODO: Hack Version!!!!!!!!
        String[] cryptoArray = Arrays.stream(Crypto.values())
                .filter(crypto -> crypto.name() != "BTC")
                .map(crypto -> crypto.name())
                .toArray(String[]::new);

        JavaRDD<Vector> vectorJavaRDD = dataFactory
                .toVectorJavaRDD(
                        dataFactory.getTableFromDB("public.daily_changes")
                                .select("BTC", cryptoArray)
                );
        JavaRDD<Vector> inputData = dataFactory.transpose(vectorJavaRDD);

        inputData.cache();

        int numClusters = 5;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(inputData.rdd(), numClusters, numIterations);

        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(inputData.rdd());
        System.out.println("Cost: " + cost);

// Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(inputData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

    }

}
