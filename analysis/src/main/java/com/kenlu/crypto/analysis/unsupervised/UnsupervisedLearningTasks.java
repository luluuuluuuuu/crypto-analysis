package com.kenlu.crypto.analysis.unsupervised;

import com.kenlu.crypto.analysis.unsupervised.correlation.CorrelationAnalysis;
import com.kenlu.crypto.analysis.unsupervised.kmeans.KMeansClusteringAnalysis;
import com.kenlu.crypto.analysis.unsupervised.pca.PrincipalComponentAnalysis;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
public class UnsupervisedLearningTasks {

    private CorrelationAnalysis correlationAnalysis;
    private PrincipalComponentAnalysis principalComponentAnalysis;
    private KMeansClusteringAnalysis kMeansClusteringAnalysis;

    public UnsupervisedLearningTasks(CorrelationAnalysis correlationAnalysis, PrincipalComponentAnalysis principalComponentAnalysis, KMeansClusteringAnalysis kMeansClusteringAnalysis) {
        this.correlationAnalysis = correlationAnalysis;
        this.principalComponentAnalysis = principalComponentAnalysis;
        this.kMeansClusteringAnalysis = kMeansClusteringAnalysis;
    }

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

    public void runCorrelation() {
        correlationAnalysis.run();
    }

    public void runPCA() {
        principalComponentAnalysis.run();
    }

    public void runKmeans() {
        kMeansClusteringAnalysis.run();
    }

}
