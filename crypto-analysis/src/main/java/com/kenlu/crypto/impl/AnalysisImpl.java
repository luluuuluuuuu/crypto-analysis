package com.kenlu.crypto.impl;

import com.kenlu.crypto.config.DBConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class AnalysisImpl implements CommandLineRunner {

    @Autowired
    protected DBConfig dbConfig;

    @Override
    public void run(String... args) {
        dbConfig.getTable("public.test_table").show();
    }

}
