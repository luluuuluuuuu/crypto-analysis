package com.kenlu.crypto.analysis.factory;

import com.kenlu.crypto.analysis.config.DBConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DataFactory {

    @Autowired
    private DBConfig dbConfig;

    public Dataset<Row> getDailyChanges() {
        List<String> cryptoList = getCryptos()
                .toJavaRDD()
                .map(row -> row.get(0).toString())
                .collect();

        return dbConfig.getTableFromDB("input.daily_changes")
                .selectExpr(cryptoList.stream().toArray(String[]::new));
    }

    public Dataset<Row> getDates() {
        return dbConfig.getTableFromDB("input.daily_changes")
                .select("date");
    }

    public Dataset<Row> getCryptos() {
        return dbConfig.getTableFromDB("input.crypto")
                .select("symbol");
    }

}
