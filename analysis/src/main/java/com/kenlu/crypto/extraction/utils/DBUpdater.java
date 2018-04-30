package com.kenlu.crypto.extraction.utils;

import com.kenlu.crypto.domain.Crypto;
import lombok.extern.slf4j.Slf4j;
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
            DateFormat f = new SimpleDateFormat("yyyy/MM/dd");
            String today = f.format(new Date(System.currentTimeMillis()));
            Map<Crypto, List<String>> cryptoDataset =
                    queryHandler.getCryptoPairs(1, System.currentTimeMillis() / 1000);
            if (cryptoDataset.size() == 0) {
                log.error("No data available...");
                return;
            }
            List<String> dates = new ArrayList<>();
            dates.add(today);
            queryHandler.insertDailyChangeQuery(cryptoDataset, dates);
        } catch (Exception e) {
            log.error("Unable to update table daily_changes...");
            e.printStackTrace();
        }
    }

}
