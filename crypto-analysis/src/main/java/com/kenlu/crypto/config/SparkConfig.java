package com.kenlu.crypto.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Slf4j
@Component
public class SparkConfig {

    @Value("${spark.conf.master}")
    private String master;
    @Value("${spark.conf.appName}")
    private String appName;
    protected SparkSession spark;

    @PostConstruct
    public void init() {
        spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
    }

}
