package com.kenlu.crypto.extraction;

import com.kenlu.crypto.extraction.utils.DBInitialiser;
import com.kenlu.crypto.extraction.utils.DBUpdater;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@Order(1)
public class ExtractionServiceImpl implements CommandLineRunner {

    private Timer timer = new Timer();
    private Calendar today = Calendar.getInstance();

    @Autowired
    private DBInitialiser dbInitialiser;
    @Autowired
    private DBUpdater dbUpdater;

    @Override
    public void run(String... args) throws Exception {
        dbInitialiser.run();
        this.update();
    }

    public void update() {
        today.set(Calendar.HOUR_OF_DAY, 2);
        today.set(Calendar.MINUTE, 0);
        today.set(Calendar.SECOND, 0);
        Date firstTime = today.getTime();
        if(firstTime.before(new Date())){
            today.add(Calendar.DATE, 1);
            firstTime = today.getTime();
        }
        timer.schedule(dbUpdater, firstTime, TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
    }

}
