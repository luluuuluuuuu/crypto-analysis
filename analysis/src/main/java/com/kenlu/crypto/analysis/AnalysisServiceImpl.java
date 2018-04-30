package com.kenlu.crypto.analysis;

import com.kenlu.crypto.analysis.unsupervised.kmeans.KMeansClusteringAnalysis;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AnalysisServiceImpl implements CommandLineRunner {

    @Autowired
    KMeansClusteringAnalysis kMeansClusteringAnalysis;

    @Override
    public void run(String... args) {
        kMeansClusteringAnalysis.run();
    }

}
