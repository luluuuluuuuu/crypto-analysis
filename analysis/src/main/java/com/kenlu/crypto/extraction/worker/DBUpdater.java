package com.kenlu.crypto.extraction.worker;

import com.kenlu.crypto.domain.OHLCV;
import com.kenlu.crypto.extraction.utils.DataExtractor;
import com.kenlu.crypto.extraction.utils.QueryHandler;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class DBUpdater {

    @Autowired
    private QueryHandler queryHandler;
    @Autowired
    private DataExtractor dataExtractor;

    public void run() {
        updateTables();
    }

    private void updateTables() {
        try {
            long lastDate = queryHandler.getLastDateFromDailyChanges().getTime();
            long today = System.currentTimeMillis();
            long startDate = new DateTime(lastDate).plusDays(1).getMillis();
            long endDate = new DateTime(today).plusDays(1).getMillis();
            int daysBetween = Days.daysBetween(new DateTime(startDate).plusDays(1), new DateTime(endDate)).getDays();
            List<OHLCV> ohlcvList = new ArrayList<>();

            queryHandler.getCryptos()
                    .forEach(crypto -> {
                        try {
                            List<OHLCV> tmpList = dataExtractor.getDailyOHLCVs(crypto, daysBetween, System.currentTimeMillis() / 1000, true);
                            ohlcvList.addAll(tmpList);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
            if (ohlcvList.size() == 0) {
                log.warn("No data available...");
                return;
            }

            queryHandler.insertOHLCVQuery(ohlcvList);
            queryHandler.insertDailyChangeQuery(ohlcvList);
        } catch (Exception e) {
            log.error("Unable to update table daily_changes...");
            e.printStackTrace();
        }
    }

}
