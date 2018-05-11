package com.kenlu.crypto.analysis;

import com.kenlu.crypto.analysis.unsupervised.UnsupervisedLearningTasks;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service("Analysis")
@DependsOn(value = {"ETL"})
public class AnalysisServiceImpl implements CommandLineRunner {

    private Timer timer = new Timer();
    private Calendar today = Calendar.getInstance();

    @Autowired
    private UnsupervisedLearningTasks unsupervisedLearningTasks;

    @Override
    public void run(String... args) {
        this.scheduleAnalysis();
    }

    public void scheduleAnalysis() {
        today.set(Calendar.HOUR_OF_DAY, 2);
        today.set(Calendar.MINUTE, 2);
        today.set(Calendar.SECOND, 0);
        Date firstTime = today.getTime();
        if(firstTime.before(new Date())){
            today.add(Calendar.DATE, 1);
            firstTime = today.getTime();
        }
        timer.schedule(unsupervisedLearningTasks, firstTime, TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
    }

}
