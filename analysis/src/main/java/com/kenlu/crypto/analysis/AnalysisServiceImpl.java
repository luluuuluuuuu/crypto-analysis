package com.kenlu.crypto.analysis;

import com.kenlu.crypto.analysis.unsupervised.correlation.CorrelationAnalysis;
import com.kenlu.crypto.analysis.unsupervised.kmeans.KMeansClusteringAnalysis;
import com.kenlu.crypto.analysis.unsupervised.pca.PrincipalComponentAnalysis;
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
    @Autowired
    private PrincipalComponentAnalysis principalComponentAnalysis;

    @Override
    public void run(String... args) {
//        kMeansClusteringAnalysis.run();
        this.analyseCorrelation();
        this.analysePCA();
    }

    public void analyseCorrelation() {
        today.set(Calendar.HOUR_OF_DAY, 2);
        today.set(Calendar.MINUTE, 10);
        today.set(Calendar.SECOND, 0);
        Date firstTime = today.getTime();
        timer.schedule(correlationAnalysis, firstTime, TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
    }

    public void analysePCA() {
        today.set(Calendar.HOUR_OF_DAY, 2);
        today.set(Calendar.MINUTE, 20);
        today.set(Calendar.SECOND, 0);
        Date firstTime = today.getTime();
        timer.schedule(principalComponentAnalysis, firstTime, TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
    }

}
