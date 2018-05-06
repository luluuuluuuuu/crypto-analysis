package com.kenlu.crypto.analysis.unsupervised.pca;

import com.kenlu.crypto.analysis.factory.DataFactory;
import com.kenlu.crypto.analysis.formatter.DataFormatter;
import com.kenlu.crypto.extraction.utils.QueryHandler;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.TimerTask;

@Component
public class PrincipalComponentAnalysis extends TimerTask {

    private static final int NUM_OF_PCS = 3;

    @Autowired
    private DataFormatter dataFormatter;
    @Autowired
    private DataFactory dataFactory;
    @Autowired
    private QueryHandler queryHandler;

    @Override
    public void run() {
        analyse();
    }

    private void analyse() {
        JavaRDD<Vector> vectorJavaRDD =
                dataFormatter.toVectorJavaRDD(dataFactory.getDailyChangeDataset());
        JavaRDD<Vector> inputData =
                dataFormatter.transpose(vectorJavaRDD);

        RowMatrix rowMatrix = new RowMatrix(inputData.rdd());
        Matrix pc = rowMatrix.computePrincipalComponents(NUM_OF_PCS);

        RowMatrix projected = rowMatrix.multiply(pc);
        List<Vector> projectedList = projected.rows().toJavaRDD().collect();
        double[][] result = new double[projectedList.size()][NUM_OF_PCS];

        for (int i = 0; i < projectedList.size(); i++) {
            for (int j = 0; j < NUM_OF_PCS; j++) {
                result[i][j] = projectedList.get(i).apply(j);
            }
        }

        queryHandler.dropTable("output", "pca");
        queryHandler.createPCATable(NUM_OF_PCS);
        queryHandler.insertPCAQuery(result);
    }
}
