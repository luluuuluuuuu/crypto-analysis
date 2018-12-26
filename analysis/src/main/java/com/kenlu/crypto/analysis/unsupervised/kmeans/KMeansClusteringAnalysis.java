package com.kenlu.crypto.analysis.unsupervised.kmeans;

import com.kenlu.crypto.analysis.factory.DataFactory;
import com.kenlu.crypto.analysis.formatter.DataFormatter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class KMeansClusteringAnalysis {

    private static final int MAX_CLUSTER_NUM = 8;
    private static final int NUM_ITERATIONS = 100;

    private DataFactory dataFactory;
    private DataFormatter dataFormatter;

    public KMeansClusteringAnalysis(DataFactory dataFactory, DataFormatter dataFormatter) {
        this.dataFactory = dataFactory;
        this.dataFormatter = dataFormatter;
    }

    public void run() {
        JavaRDD<Vector> vectorJavaRDD =
                dataFormatter.toVectorJavaRDD(dataFactory.getCryptoPCADataset());

        KMeans kMeans = new KMeans()
                .setK(1)
                .setMaxIterations(NUM_ITERATIONS)
                .setSeed(1990);
        KMeansModel clusters = kMeans.run(vectorJavaRDD.rdd());
        double cost = clusters.computeCost(vectorJavaRDD.rdd());
        JavaRDD<Integer> predictions = clusters.predict(vectorJavaRDD);
        Vector[] centers = clusters.clusterCenters();

        for (int i = 2; i < MAX_CLUSTER_NUM; i++) {
            KMeans tmpKMeans = new KMeans()
                    .setK(i)
                    .setMaxIterations(NUM_ITERATIONS)
                    .setSeed(1990);
            KMeansModel tmpClusters = tmpKMeans.run(vectorJavaRDD.rdd());
            JavaRDD<Integer> tmpPredictions = tmpClusters.predict(vectorJavaRDD);
            Vector[] tmpCenters = tmpClusters.clusterCenters();
            double tmpCost = tmpClusters.computeCost(vectorJavaRDD.rdd());

            if (tmpCost < cost) {
                cost = tmpCost;
                predictions = tmpPredictions;
                centers = tmpCenters;
            }
        }

        String WSSSE = String.format("%.15f", cost);

        this.saveCentersAndCost(centers, WSSSE, "crypto_kmeans_centers");
        this.saveClusters(predictions, "crypto_kmeans_clusters");

    }

    private void saveClusters(JavaRDD<Integer> predictions1, String table) {
        JavaRDD<Row> predictions = dataFormatter.intToRowJavaRDD(predictions1);
        List<String> cryptoList = dataFactory.getCryptoDataset()
                .toJavaRDD()
                .map(row -> row.get(0).toString())
                .collect();
        predictions = dataFormatter.addFirstValueToRows(predictions, cryptoList);
        List<StructField> fields = new ArrayList<>();

        StructField cryptoField =
                DataTypes.createStructField("crypto", DataTypes.StringType, false);
        StructField field =
                DataTypes.createStructField("cluster", DataTypes.StringType, false);
        fields.add(cryptoField);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> rowDataset = dataFormatter.toRowDataset(predictions, schema);

        dataFactory.writeOutputToDB(rowDataset, table, SaveMode.Overwrite);
    }

    private void saveCentersAndCost(Vector[] centers, String WSSSE, String table) {
        List<String> lastValues = new ArrayList<>();
        lastValues.add(WSSSE);
        for (int i = 1; i < MAX_CLUSTER_NUM; i++) {
            lastValues.add(null);
        }
        JavaRDD<Row> rowJavaRDD = dataFormatter.toRowJavaRDD(centers);
        rowJavaRDD = dataFormatter.addLastValueToRows(rowJavaRDD, lastValues);

        List<StructField> fields = new ArrayList<>();

        for (int i = 0; i < rowJavaRDD.first().size() - 1; i++) {
            StructField field =
                    DataTypes.createStructField("feature" + Integer.toString(i), DataTypes.StringType, true);
            fields.add(field);
        }
        StructField WSSSEField =
                DataTypes.createStructField("WSSSE", DataTypes.StringType, true);
        fields.add(WSSSEField);

        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> rowDataset = dataFormatter.toRowDataset(rowJavaRDD, schema);

        dataFactory.writeOutputToDB(rowDataset, table, SaveMode.Overwrite);
    }

}
