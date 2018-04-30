package com.kenlu.crypto.extraction.utils;

import com.kenlu.crypto.domain.Crypto;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class DBInitialiser {

    private static final long TO_TIMESTAMP = System.currentTimeMillis() / 1000;
    private static final long FROM_TIMESTAMP = 1483228800;

    private Map<Crypto, List<String>> initCryptoDataset;

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private QueryHandler queryHandler;

    public void run() throws Exception {
        log.info("Initiating tables...");
        try {
            initTableIfNotExist("crypto");
            initTableIfNotExist("daily_changes");
            log.info("Tables are initiated");
        } catch (SQLException e) {
            log.error("Tables cannot be initiated");
            e.printStackTrace();
        }
    }

    private void initTableIfNotExist(String table) throws Exception {
        log.info("Initiating table {}...", table);
        boolean isTableExist = this.jdbcTemplate
                .getDataSource()
                .getConnection()
                .getMetaData()
                .getTables(null, null, table, null)
                .next();
        if (!isTableExist) {
            DateTime toDate = new DateTime(TO_TIMESTAMP * 1000);
            DateTime fromDate = new DateTime(FROM_TIMESTAMP * 1000);
            int initNumOfDays = Days.daysBetween(fromDate, toDate).getDays();
            if (this.initCryptoDataset == null) {
                this.initCryptoDataset = queryHandler.getCryptoPairs(initNumOfDays, TO_TIMESTAMP);
            }
            switch (table) {
                case "daily_changes" :
                    List<String> dates = queryHandler.getDatesBetween(FROM_TIMESTAMP, TO_TIMESTAMP);
                    queryHandler.createDailyChangeTable(initCryptoDataset);
                    queryHandler.insertDailyChangeQuery(initCryptoDataset, dates);
                    break;
                case "crypto" :
                    queryHandler.createCryptoTable();
                    queryHandler.insertCryptoQuery(initCryptoDataset);
                    break;
                default:
                    break;
            }
        }
        if (table.equals("daily_changes") || table.equals("crypto")) {
            log.info("Table {} is initiated", table);
        } else {
            log.error("Cannot initiate table {}", table);
        }
    }

}
