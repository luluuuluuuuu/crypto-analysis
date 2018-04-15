package com.kenlu.crypto.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Slf4j
@Component
public class DBConfig {

    @Value("${spring.datasource.url}")
    private String url;
    @Value("${spring.datasource.username}")
    private String username;
    @Value("${spring.datasource.password}")
    private String password;
    @Autowired
    private SparkConfig sparkConfig;
    private Properties connectionProperties;

    public DBConfig() {
        connectionProperties = new Properties();
    }

    @PostConstruct
    public void init() {
        connectionProperties.put("user", username);
        connectionProperties.put("password", password);
    }

    public Dataset<Row> getTable(String table) {
        log.info("Reading {}...", table);
        return sparkConfig.spark.read()
                .jdbc(url, table, connectionProperties);
    }

}
