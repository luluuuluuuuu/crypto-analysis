package com.kenlu.crypto.analysis;

import com.kenlu.crypto.analysis.unsupervised.correlation.CorrelationAnalysis;
import com.kenlu.crypto.analysis.unsupervised.kmeans.KMeansClusteringAnalysis;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class AnalysisServiceImpl implements CommandLineRunner {

    private Timer timer = new Timer();
    private Calendar today = Calendar.getInstance();

    @Autowired
    private KMeansClusteringAnalysis kMeansClusteringAnalysis;
    @Autowired
    private CorrelationAnalysis correlationAnalysis;

    @Override
    public void run(String... args) {
//        kMeansClusteringAnalysis.run();
        this.analyseCorrelation();
    }

    public void analyseCorrelation() {
        today.set(Calendar.HOUR_OF_DAY, 2);
        today.set(Calendar.MINUTE, 10);
        today.set(Calendar.SECOND, 0);
        Date firstTime = today.getTime();
        timer.schedule(correlationAnalysis, firstTime, TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
    }

}
