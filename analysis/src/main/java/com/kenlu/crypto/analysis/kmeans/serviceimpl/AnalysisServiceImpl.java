package com.kenlu.crypto.analysis.kmeans.serviceimpl;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class AnalysisServiceImpl implements CommandLineRunner {

    private static final int NUM_CLUSTERS = 3;
    private static final int NUM_ITERATIONS = 20;

    @Autowired
    private DataFactory dataFactory;

    @Override
    public void run(String... args) {

        List<String> cryptos = dataFactory.getTableFromDB("public.crypto")
                .select("symbol")
                .toJavaRDD()
                .map(row -> row.get(0).toString())
                .collect();

        Dataset<Row> table = dataFactory.getTableFromDB("public.daily_changes")
                .selectExpr(cryptos.stream().toArray(String[]::new));

        JavaRDD<Vector> vectorJavaRDD = dataFactory
                .toVectorJavaRDD(table);
        JavaRDD<Vector> inputData = dataFactory.transpose(vectorJavaRDD);

        inputData.cache();


        KMeansModel clusters = KMeans.train(inputData.rdd(), NUM_CLUSTERS, NUM_ITERATIONS);

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
