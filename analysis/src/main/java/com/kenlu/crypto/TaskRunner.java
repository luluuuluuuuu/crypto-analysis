package com.kenlu.crypto;

import com.kenlu.crypto.analysis.AnalysisServiceImpl;
import com.kenlu.crypto.extraction.ExtractionServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service("Main")
public class TaskRunner implements CommandLineRunner {

    private Timer timer = new Timer();
    private Calendar today = Calendar.getInstance();

    @Autowired
    private ExtractionServiceImpl extractionService;
    @Autowired
    private AnalysisServiceImpl analysisService;

    @Override
    public void run(String... args) {
        CompletableFuture
                .runAsync(extractionService::initialize)
                .thenRunAsync(this::schedule);
    }

    private void schedule() {
        today.set(Calendar.HOUR_OF_DAY, 2);
        today.set(Calendar.MINUTE, 0);
        today.set(Calendar.SECOND, 0);
        Date firstTime = today.getTime();
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    CompletableFuture.runAsync(extractionService::update)
                            .thenRunAsync(analysisService::runAll)
                            .get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error(e.getMessage());
                }
            }
        };
        timer.schedule(timerTask, firstTime, TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
    }
}
