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

@Slf4j
@Component
public class KMeansClusteringAnalysis {

    private static final int NUM_CLUSTERS = 3;
    private static final int NUM_ITERATIONS = 20;

    @Autowired
    private DataFactory dataFactory;
    @Autowired
    private DataFormatter dataFormatter;

    public void run() {

//        Schema schema = new Schema.Builder().build();
//        TransformProcess transformProcess = new TransformProcess.Builder(schema).build();
//        INDArray indArray = Nd4j.create(3,3).transpose();
//        Point.toPoints(indArray);

        JavaRDD<Vector> vectorJavaRDD =
                dataFormatter.toVectorJavaRDD(dataFactory.getDailyChangeDataset());
        JavaRDD<Vector> inputData =
                dataFormatter.transpose(vectorJavaRDD);

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
