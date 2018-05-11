package com.kenlu.crypto.analysis.formatter;

import com.kenlu.crypto.analysis.config.SparkConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class DataFormatter {

    @Autowired
    private SparkConfig sparkConfig;

    private JavaSparkContext javaSparkContext;

    @PostConstruct
    private void init() {
        this.javaSparkContext = new JavaSparkContext(sparkConfig.sparkSession.sparkContext());
    }

    public JavaRDD<Vector> transpose(JavaRDD<Vector> vectorJavaRDD) {
        List<Vector> originalList = vectorJavaRDD.collect();
        List<Vector> transformedList;
        double[][] transposeDoubles = new double[originalList.get(0).size()][originalList.size()];

        for (int i = 0; i < originalList.size(); i++) {
            double[] row = originalList.get(i).toArray();
            for (int j = 0; j < row.length; j++) {
                transposeDoubles[j][i] = row[j];
            }
        }

        transformedList = Arrays
                .stream(transposeDoubles)
                .map(Vectors::dense)
                .collect(Collectors.toList());

        return javaSparkContext.parallelize(transformedList);
    }

    public JavaRDD<Vector> toVectorJavaRDD(Dataset<Row> dataset) {
        return dataset
                .toJavaRDD()
                .map(row -> {
                    double[] values = new double[row.length()];
                    for (int i = 0; i < row.length(); i++) {
                        values[i] = Double.parseDouble(row.get(i).toString());
                    }
                    return Vectors.dense(values);
                });
    }

    public JavaRDD<Row> toRowJavaRDD(String[][] strings) {
        List<Row> rowList = new ArrayList<>();
        for (int i = 0; i < strings.length; i++) {
            rowList.add(RowFactory.create(strings[i]));
        }
        return javaSparkContext.parallelize(rowList);
    }

    public Dataset<Row> toRowDataset(JavaRDD<Row> rowJavaRDD, StructType structType) {
        return sparkConfig.sparkSession.createDataFrame(rowJavaRDD, structType);
    }

}
