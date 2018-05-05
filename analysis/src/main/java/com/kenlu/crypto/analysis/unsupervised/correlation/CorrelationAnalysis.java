package com.kenlu.crypto.analysis.unsupervised.correlation;

import com.kenlu.crypto.analysis.factory.DataFactory;
import com.kenlu.crypto.analysis.formatter.DataFormatter;
import com.kenlu.crypto.extraction.utils.QueryHandler;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.TimerTask;

@Component
public class CorrelationAnalysis extends TimerTask {

    @Autowired
    private DataFactory dataFactory;
    @Autowired
    private DataFormatter dataFormatter;
    @Autowired
    private QueryHandler queryHandler;

    @Override
    public void run() {
        analyse();
    }

    private void analyse() {
        JavaRDD<Vector> vectorJavaRDD =
                dataFormatter.toVectorJavaRDD(dataFactory.getDailyChangeDataset());
        Matrix correlation = Statistics.corr(vectorJavaRDD.rdd());

        double[][] matrix = new double[correlation.numRows()][correlation.numCols()];

        for (int i = 0; i < correlation.numRows(); i++) {
            for (int j = 0; j < correlation.numCols(); j++) {
                matrix[i][j] = correlation.apply(i, j);
            }
        }

        queryHandler.dropTable("output", "correlation");
        queryHandler.createCorrelationTable();
        queryHandler.insertCorrelationQuery(matrix);
    }
}
