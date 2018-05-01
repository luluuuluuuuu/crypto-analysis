package com.kenlu.crypto.extraction;

import com.kenlu.crypto.extraction.utils.DBInitialiser;
import com.kenlu.crypto.extraction.utils.DBUpdater;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@Order(1)
public class ExtractionServiceImpl implements CommandLineRunner {

    @Autowired
    private DBInitialiser dbInitialiser;
    @Autowired
    private DBUpdater dbUpdater;

    @Override
    public void run(String... args) throws Exception {
        dbInitialiser.run();
        update();
    }

    public void update() {
        Timer timer = new Timer();
        Calendar today = Calendar.getInstance();

        today.set(Calendar.HOUR_OF_DAY, 1);
        today.set(Calendar.MINUTE, 0);
        today.set(Calendar.SECOND, 0);
        timer.schedule(dbUpdater, today.getTime(), TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
    }

}
