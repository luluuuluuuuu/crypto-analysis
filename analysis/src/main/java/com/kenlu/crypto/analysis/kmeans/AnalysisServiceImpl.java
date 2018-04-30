package com.kenlu.crypto.analysis.kmeans;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import scala.Int;

import java.util.List;

@Slf4j
@Service
@Order(2)
public class AnalysisServiceImpl implements CommandLineRunner {

    private static final int NUM_CLUSTERS = 3;
    private static final int NUM_ITERATIONS = 20;

    @Autowired
    private KmeansDataFormatter kmeansDataFormatter;

    @Override
    public void run(String... args) {

        List<String> cryptos = kmeansDataFormatter.getTableFromDB("input.crypto")
                .select("symbol")
                .toJavaRDD()
                .map(row -> row.get(0).toString())
                .collect();

        Dataset<Row> table = kmeansDataFormatter.getTableFromDB("input.daily_changes")
                .selectExpr(cryptos.stream().toArray(String[]::new));

        JavaRDD<Vector> vectorJavaRDD = kmeansDataFormatter
                .toVectorJavaRDD(table);
        JavaRDD<Vector> inputData = kmeansDataFormatter.transpose(vectorJavaRDD);

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
