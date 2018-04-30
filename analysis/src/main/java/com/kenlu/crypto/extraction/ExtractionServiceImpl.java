package com.kenlu.crypto.extraction;

import com.kenlu.crypto.extraction.utils.DBInitialiser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Order(1)
public class ExtractionServiceImpl implements CommandLineRunner {

    @Autowired
    DBInitialiser dbInitialiser;

    @Override
    public void run(String... args) throws Exception {
        dbInitialiser.initTables();
        // TODO Insert daily data every day
    }

}
