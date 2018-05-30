package com.kenlu.crypto.extraction;

import com.kenlu.crypto.extraction.worker.DBInitializer;
import com.kenlu.crypto.extraction.worker.DBUpdater;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service("ETL")
public class ExtractionServiceImpl {

    @Autowired
    private DBInitializer dbInitializer;
    @Autowired
    private DBUpdater dbUpdater;

    public void initialize() {
        try {
            dbInitializer.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void update() {
        dbUpdater.run();
    }

}
