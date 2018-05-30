package com.kenlu.crypto.analysis;

import com.kenlu.crypto.analysis.unsupervised.UnsupervisedLearningTasks;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service("Analysis")
public class AnalysisServiceImpl {

    @Autowired
    private UnsupervisedLearningTasks unsupervisedLearningTasks;

    public void runAll() {
        unsupervisedLearningTasks.run();
    }

    public void runCorrelation() {
        unsupervisedLearningTasks.runCorrelation();
    }

    public void runPCA() {
        unsupervisedLearningTasks.runPCA();
    }

    public void runKmeans() {
        unsupervisedLearningTasks.runKmeans();
    }

}
