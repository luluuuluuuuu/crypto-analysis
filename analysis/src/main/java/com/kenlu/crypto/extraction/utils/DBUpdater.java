package com.kenlu.crypto.extraction.utils;

import com.kenlu.crypto.domain.Crypto;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
@Component
public class DBUpdater extends TimerTask {

    @Autowired
    private QueryHandler queryHandler;

    @Override
    public void run() {
        updateDailyChanges();
    }

    private void updateDailyChanges() {
        try {
            DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
            long lastDate = queryHandler.getLastDateFromDailyChanges().getTime();
            long today = System.currentTimeMillis();
            long startDate = new DateTime(lastDate).plusDays(1).getMillis();
            long endDate = new DateTime(today).plusDays(1).getMillis();
            int daysBetween = Days.daysBetween(new DateTime(startDate).plusDays(1), new DateTime(endDate)).getDays();
            List<Crypto> cryptos = queryHandler.getCryptos();
            Map<Crypto, List<String>> cryptoDataset =
                    queryHandler.getCryptoPairs(cryptos, daysBetween, System.currentTimeMillis() / 1000, true);
            if (cryptoDataset.size() == 0) {
                log.error("No data available...");
                return;
            }
            List<String> dates = queryHandler.getDatesBetween(startDate, endDate);
            queryHandler.insertDailyChangeQuery(cryptoDataset, dates);
        } catch (Exception e) {
            log.error("Unable to update table daily_changes...");
            e.printStackTrace();
        }
    }

}
