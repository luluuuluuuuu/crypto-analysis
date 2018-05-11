package com.kenlu.crypto.analysis.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Slf4j
@Component
public class DBConfig {

    @Autowired
    private SparkConfig sparkConfig;
    @Value("${spring.datasource.url}")
    private String url;
    @Value("${spring.datasource.username}")
    private String username;
    @Value("${spring.datasource.password}")
    private String password;
    private Properties connectionProperties;

    public DBConfig() {
        connectionProperties = new Properties();
    }

    @PostConstruct
    private void init() {
        connectionProperties.put("user", username);
        connectionProperties.put("password", password);
    }

    public String getUrl() {
        return url;
    }

    public Properties getConnectionProperties() {
        return connectionProperties;
    }

    public Dataset<Row> readDatasetFromDB(String schema, String table) {
        log.info("Reading {}...", schema + "." + table);
        return sparkConfig.sparkSession.read()
                .jdbc(url, schema + "." + table, getConnectionProperties());
    }

    public void writeTableToDB(Dataset<Row> df, String schema, String table, SaveMode saveMode) {
        log.info("Writing {}...", schema + "." + table);
        df.write().mode(saveMode).jdbc(url, schema + "." + table, getConnectionProperties());
    }

}
