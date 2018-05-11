package com.kenlu.crypto.analysis.unsupervised;

import com.kenlu.crypto.analysis.unsupervised.correlation.CorrelationAnalysis;
import com.kenlu.crypto.analysis.unsupervised.kmeans.KMeansClusteringAnalysis;
import com.kenlu.crypto.analysis.unsupervised.pca.PrincipalComponentAnalysis;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
public class UnsupervisedLearningTasks extends TimerTask {

    @Autowired
    CorrelationAnalysis correlationAnalysis;
    @Autowired
    PrincipalComponentAnalysis principalComponentAnalysis;
    @Autowired
    KMeansClusteringAnalysis kMeansClusteringAnalysis;

    @Override
    public void run() {
        CompletableFuture<Void> future = CompletableFuture
                .runAsync(() -> principalComponentAnalysis.run())
                .thenRun(() -> kMeansClusteringAnalysis.run());

        correlationAnalysis.run();
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

}
