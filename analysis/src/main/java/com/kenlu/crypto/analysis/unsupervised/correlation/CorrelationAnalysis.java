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
import org.springframework.stereotype.Component;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;

@Component
public class CorrelationAnalysis {

    private DataFactory dataFactory;
    private DataFormatter dataFormatter;

    public CorrelationAnalysis(DataFactory dataFactory, DataFormatter dataFormatter) {
        this.dataFactory = dataFactory;
        this.dataFormatter = dataFormatter;
    }

    public void run() {
        JavaRDD<Vector> vectorJavaRDD =
                dataFormatter.toVectorJavaRDD(dataFactory.getCryptoDailyChangeDataset());
        Matrix correlation = Statistics.corr(vectorJavaRDD.rdd());
        List<Vector> vectorList =
                new ArrayList<>(JavaConverters.asJavaCollectionConverter(correlation.rowIter().toList()).asJavaCollection());

        this.save(vectorList, "crypto_correlation");
    }

    private void save(List<Vector> vectorList, String table) {
        List<String> cryptoList = dataFactory.getCryptoDataset()
                .toJavaRDD()
                .map(row -> row.get(0).toString())
                .collect();
        List<StructField> fields = new ArrayList<>();

        StructField cryptoField =
                DataTypes.createStructField("crypto", DataTypes.StringType, false);
        fields.add(cryptoField);

        cryptoList.forEach(crypto -> {
                        StructField field =
                                DataTypes.createStructField(crypto, DataTypes.StringType, false);
                        fields.add(field);
        });

        StructType schema = DataTypes.createStructType(fields);
        JavaRDD<Row> rowJavaRDD = dataFormatter.toRowJavaRDD(vectorList);
        rowJavaRDD = dataFormatter.addFirstValueToRows(rowJavaRDD, cryptoList);
        Dataset<Row> rowDataset = dataFormatter.toRowDataset(rowJavaRDD, schema);

        dataFactory.writeOutputToDB(rowDataset, table, SaveMode.Overwrite);
    }
}
