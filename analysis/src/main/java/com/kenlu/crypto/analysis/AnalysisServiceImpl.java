package com.kenlu.crypto.analysis;

import com.kenlu.crypto.analysis.unsupervised.UnsupervisedLearningTasks;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service("Analysis")
public class AnalysisServiceImpl {

    private UnsupervisedLearningTasks unsupervisedLearningTasks;

    public AnalysisServiceImpl(UnsupervisedLearningTasks unsupervisedLearningTasks) {
        this.unsupervisedLearningTasks = unsupervisedLearningTasks;
    }

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
