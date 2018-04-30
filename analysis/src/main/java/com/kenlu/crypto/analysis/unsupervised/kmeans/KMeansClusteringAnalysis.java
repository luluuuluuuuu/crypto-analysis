package com.kenlu.crypto.analysis.unsupervised.kmeans;

import com.kenlu.crypto.analysis.factory.DataFactory;
import com.kenlu.crypto.analysis.formatter.DataFormatter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.stat.Statistics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Int;

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

        JavaRDD<Vector> vectorJavaRDD = dataFormatter
                .toVectorJavaRDD(dataFactory.getDailyChanges());
        JavaRDD<Vector> inputData = dataFormatter.transpose(vectorJavaRDD);

        // Create a RowMatrix from JavaRDD<Vector>.
        RowMatrix mat = new RowMatrix(inputData.rdd());

        // Compute the top 4 principal components.
        // Principal components are stored in a local dense matrix.
        Matrix pc = mat.computePrincipalComponents(2);
        Matrix covariance = new RowMatrix(vectorJavaRDD.rdd()).computeCovariance();
        Matrix correlation = Statistics.corr(vectorJavaRDD.rdd());

        // Project the rows to the linear space spanned by the top 4 principal components.
        RowMatrix projected = mat.multiply(pc);

//        projected.rows().toJavaRDD().foreach(x -> {
//            System.out.println(Arrays.toString(x.toArray()));
//        });

//        System.out.println(covariance.numRows());
//        System.out.println(covariance.numCols());
//        System.out.println(covariance.toString(Int.MaxValue(), Int.MaxValue()));
        System.out.println(correlation.toString(Int.MaxValue(), Int.MaxValue()));
//        System.out.println(projected.numRows());

//        inputData.cache();
//
//
        KMeansModel clusters = KMeans.train(projected.rows(), NUM_CLUSTERS, NUM_ITERATIONS);

        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(projected.rows());
        System.out.println("Cost: " + cost);

// Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(projected.rows());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

    }

}
