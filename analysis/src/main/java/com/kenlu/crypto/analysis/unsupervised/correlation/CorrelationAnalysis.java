package com.kenlu.crypto.analysis.unsupervised.correlation;

import com.kenlu.crypto.analysis.factory.DataFactory;
import com.kenlu.crypto.analysis.formatter.DataFormatter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
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

@Component
public class CorrelationAnalysis {

    @Autowired
    private DataFactory dataFactory;
    @Autowired
    private DataFormatter dataFormatter;

    public void run() {
        JavaRDD<Vector> vectorJavaRDD =
                dataFormatter.toVectorJavaRDD(dataFactory.getDailyChangeDataset());
        Matrix correlation = Statistics.corr(vectorJavaRDD.rdd());
        List<String> cryptoList = dataFactory.getCryptoDataset()
                .toJavaRDD()
                .map(row -> row.get(0).toString())
                .collect();

        int columnNums = correlation.numCols() + 1;
        String[][] matrix = new String[correlation.numRows()][columnNums];

        for (int i = 0; i < correlation.numRows(); i++) {
            matrix[i][0] = cryptoList.get(i);
            for (int j = 1; j < columnNums; j++) {
                matrix[i][j] = String.format("%.15f", correlation.apply(i, j-1));
            }
        }

        this.save(cryptoList, matrix);
    }

    private void save(List<String> cryptoList, String[][] matrix) {
        List<StructField> fields = new ArrayList<>();

        StructField cryptoField =
                DataTypes.createStructField("crypto", DataTypes.StringType, false);
        fields.add(cryptoField);

        cryptoList.stream()
                .forEach(crypto -> {
                    StructField field =
                            DataTypes.createStructField(crypto, DataTypes.StringType, false);
                    fields.add(field);
                });

        StructType schema = DataTypes.createStructType(fields);
        JavaRDD<Row> rowJavaRDD = dataFormatter.toRowJavaRDD(matrix);
        Dataset<Row> rowDataset = dataFormatter.toRowDataset(rowJavaRDD, schema);

        dataFactory.writeOutputToDB(rowDataset, "correlation", SaveMode.Overwrite);
    }
}
