package com.kenlu.crypto.analysis.unsupervised.pca;

import com.kenlu.crypto.analysis.factory.DataFactory;
import com.kenlu.crypto.analysis.formatter.DataFormatter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.feature.Normalizer;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
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
public class PrincipalComponentAnalysis {

    private static final int NUM_OF_PCS = 3;

    @Autowired
    private DataFormatter dataFormatter;
    @Autowired
    private DataFactory dataFactory;

    public void run() {
        JavaRDD<Vector> vectorJavaRDD =
                dataFormatter.toVectorJavaRDD(dataFactory.getDailyChangeDataset());
        JavaRDD<Vector> normalisedRDD =
                new Normalizer().transform(vectorJavaRDD);
        JavaRDD<Vector> inputData =
                dataFormatter.transpose(normalisedRDD);

        RowMatrix rowMatrix = new RowMatrix(inputData.rdd());
        Matrix pc = rowMatrix.computePrincipalComponents(NUM_OF_PCS);
        RowMatrix projected = rowMatrix.multiply(pc);

        this.save(projected);
    }

    private void save(RowMatrix projected) {
        List<String> cryptoList = dataFactory.getCryptoDataset()
                .toJavaRDD()
                .map(row -> row.get(0).toString())
                .collect();

        JavaRDD<Row> rowJavaRDD = dataFormatter.toRowJavaRDD(projected.rows().toJavaRDD());
        rowJavaRDD = dataFormatter.addFirstValueToRows(rowJavaRDD, cryptoList);

        List<StructField> fields = new ArrayList<>();

        StructField cryptoField =
                DataTypes.createStructField("crypto", DataTypes.StringType, false);
        fields.add(cryptoField);

        for (int i = 0; i < NUM_OF_PCS; i++) {
            StructField field =
                    DataTypes.createStructField(Integer.toString(i), DataTypes.StringType, false);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> rowDataset = dataFormatter.toRowDataset(rowJavaRDD, schema);

        dataFactory.writeOutputToDB(rowDataset, "pca", SaveMode.Overwrite);
    }
}
