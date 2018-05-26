package com.kenlu.crypto.analysis.factory;

import static org.apache.spark.sql.functions.*;

import com.kenlu.crypto.analysis.config.DBConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DataFactory {

    @Autowired
    private DBConfig dbConfig;

    public Dataset<Row> getDailyChangeDataset() {
        List<String> cryptoList = getCryptoDataset()
                .toJavaRDD()
                .map(row -> row.get(0).toString())
                .collect();

        return dbConfig.readDatasetFromDB("input", "daily_changes")
                .selectExpr(cryptoList.stream().toArray(String[]::new))
                .orderBy(asc("date"));
    }

    public Dataset<Row> getDateDataset() {
        return dbConfig.readDatasetFromDB("input", "daily_changes")
                .select("date")
                .orderBy(asc("date"));
    }

    public Dataset<Row> getCryptoDataset() {
        return dbConfig.readDatasetFromDB("input", "crypto")
                .select("symbol")
                .orderBy(asc("symbol"));
    }

    public Dataset<Row> getPCADataset() {
        return dbConfig.readDatasetFromDB("output", "pca")
                .select("feature0", "feature1", "feature2")
                .orderBy(asc("crypto"));
    }

    public void writeOutputToDB(Dataset<Row> df, String table, SaveMode saveMode) {
        dbConfig.writeTableToDB(df, "output", table, saveMode);
    }

}
