package com.kenlu.crypto;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Crypto Analysis")
                .getOrCreate();
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "username");
        connectionProperties.put("password", "password");
        Dataset<Row> jdbcDF = spark.read()
                .jdbc("jdbc:postgresql://localhost:5432/postgres", "schema.tablename", connectionProperties);
    }
}
