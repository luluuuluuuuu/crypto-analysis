package com.kenlu.crypto.extraction.worker;

import com.kenlu.crypto.domain.OHLC;
import com.kenlu.crypto.extraction.utils.DataExtractor;
import com.kenlu.crypto.extraction.utils.QueryHandler;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class DBUpdater {

    private QueryHandler queryHandler;
    private DataExtractor dataExtractor;

    public DBUpdater(QueryHandler queryHandler, DataExtractor dataExtractor) {
        this.queryHandler = queryHandler;
        this.dataExtractor = dataExtractor;
    }

    public void run() {
        updateTables();
    }

    private void updateTables() {
        try {
            long lastDate = queryHandler.getLastDateFromDailyChanges().getTime();
            long today = System.currentTimeMillis();
            long startDate = new DateTime(lastDate).plusDays(1).getMillis();
            long endDate = new DateTime(today).plusDays(1).getMillis();
            int daysBetween = Days.daysBetween(new DateTime(startDate), new DateTime(endDate)).minus(1).getDays();
            List<OHLC> OHLCList = new ArrayList<>();

            if (daysBetween <= 0) {
                log.warn("No data available...");
                return;
            }
            queryHandler.getCryptos()
                    .forEach(crypto -> {
                        try {
                            List<OHLC> tmpList = dataExtractor.getCryptoDailyOHLCVs(crypto, daysBetween, System.currentTimeMillis() / 1000, true);
                            OHLCList.addAll(tmpList);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });

            queryHandler.insertCryptoOHLCVQuery(OHLCList);
            queryHandler.insertCryptoDailyChangeQuery(OHLCList);
        } catch (Exception e) {
            log.error("Unable to update tables...");
            e.printStackTrace();
        }
    }

}
