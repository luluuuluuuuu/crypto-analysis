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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class KMeansClusteringAnalysis {

    private static final int NUM_CLUSTERS = 12;
    private static final int NUM_ITERATIONS = 10000;

    @Autowired
    private DataFactory dataFactory;
    @Autowired
    private DataFormatter dataFormatter;

    public void run() {
        JavaRDD<Vector> vectorJavaRDD =
                dataFormatter.toVectorJavaRDD(dataFactory.getPCADataset());

        KMeans kMeans = new KMeans()
                .setK(NUM_CLUSTERS)
                .setMaxIterations(NUM_ITERATIONS)
                .setSeed(1990);

        KMeansModel clusters = kMeans.run(vectorJavaRDD.rdd());
        Vector[] centres = clusters.clusterCenters();
        String WSSSE = String.format("%.15f", clusters.computeCost(vectorJavaRDD.rdd()));
        JavaRDD<Integer> predictions1 = clusters.predict(vectorJavaRDD);

        this.saveCentersAndCost(centres, WSSSE, "kmeans_centers");
        this.saveClusters(predictions1, "kmeans_clusters");

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
        for (int i = 1; i < NUM_CLUSTERS; i++) {
            lastValues.add(null);
        }
        JavaRDD<Row> rowJavaRDD = dataFormatter.toRowJavaRDD(centers);
        rowJavaRDD = dataFormatter.addLastValueToRows(rowJavaRDD, lastValues);

        List<StructField> fields = new ArrayList<>();

        for (int i = 0; i < rowJavaRDD.first().size() - 1; i++) {
            StructField field =
                    DataTypes.createStructField(Integer.toString(i), DataTypes.StringType, true);
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
