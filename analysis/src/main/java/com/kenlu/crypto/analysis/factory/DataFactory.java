package com.kenlu.crypto.analysis.factory;

import static org.apache.spark.sql.functions.*;

import com.kenlu.crypto.analysis.config.DBConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DataFactory {

    private DBConfig dbConfig;

    public DataFactory(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public Dataset<Row> getCryptoDailyChangeDataset() {
        List<String> cryptoList = getCryptoDataset()
                .toJavaRDD()
                .map(row -> row.get(0).toString())
                .collect();

        return dbConfig.readDatasetFromDB("input", "crypto_daily_changes")
                .selectExpr(cryptoList.stream().toArray(String[]::new))
                .orderBy(asc("date"));
    }

    public Dataset<Row> getCryptoDateDataset() {
        return dbConfig.readDatasetFromDB("input", "crypto_daily_changes")
                .select("date")
                .orderBy(asc("date"));
    }

    public Dataset<Row> getCryptoDataset() {
        return dbConfig.readDatasetFromDB("input", "crypto")
                .select("symbol")
                .orderBy(asc("symbol"));
    }

    public Dataset<Row> getCryptoPCADataset() {
        return dbConfig.readDatasetFromDB("output", "crypto_pca")
                .select("feature0", "feature1")
                .orderBy(asc("crypto"));
    }

    public void writeOutputToDB(Dataset<Row> df, String table, SaveMode saveMode) {
        dbConfig.writeTableToDB(df, "output", table, saveMode);
    }

}
