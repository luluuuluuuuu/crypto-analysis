package com.kenlu.crypto.analysis.kmeans.serviceimpl.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Component
public class DBConfig {

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

}
