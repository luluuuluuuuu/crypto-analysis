package com.kenlu.crypto.analysis.unsupervised.pca;

import com.kenlu.crypto.analysis.factory.DataFactory;
import com.kenlu.crypto.analysis.formatter.DataFormatter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.TimerTask;

@Component
public class PrincipalComponentAnalysis extends TimerTask {

    @Autowired
    private DataFormatter dataFormatter;
    @Autowired
    private DataFactory dataFactory;

    @Override
    public void run() {
        analyse();
    }

    private void analyse() {
        JavaRDD<Vector> vectorJavaRDD =
                dataFormatter.toVectorJavaRDD(dataFactory.getDailyChangeDataset());
        JavaRDD<Vector> inputData =
                dataFormatter.transpose(vectorJavaRDD);
        List<String> cryptos =
                dataFactory.getCryptoDataset().toJavaRDD().map(x -> (String) x.get(0)).collect();

        RowMatrix rowMatrix = new RowMatrix(inputData.rdd());
        Matrix pc = rowMatrix.computePrincipalComponents(3);

        RowMatrix projected = rowMatrix.multiply(pc);
        List<Vector> projectedList = projected.rows().toJavaRDD().collect();
        System.out.println("Crypto, x, y, z");
        for (int i = 0; i < projectedList.size(); i++) {
            System.out.print(cryptos.get(i));
            System.out.print(", ");
            Arrays.stream(projectedList.get(i).toArray())
                    .forEach(y -> {
                        System.out.print(y);
                        System.out.print(", ");
                    });
            System.out.println(" ");
        }

    }

}
