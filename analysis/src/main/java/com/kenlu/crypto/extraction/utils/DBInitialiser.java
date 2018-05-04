package com.kenlu.crypto.extraction.utils;

import com.kenlu.crypto.domain.Crypto;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            initTableIfNotExist("input", "crypto");
            initTableIfNotExist("input", "daily_changes");
            log.info("Tables are initiated");
        } catch (SQLException e) {
            log.error("Tables cannot be initiated");
            e.printStackTrace();
        }
    }

    private void initTableIfNotExist(String schema, String table) throws Exception {
        String theTable = schema + "." + table;
        boolean isTableExist = this.jdbcTemplate
                .getDataSource()
                .getConnection()
                .getMetaData()
                .getTables(null, schema, table, null)
                .next();

        log.info("Initiating table {}...", theTable);

        if (!isTableExist) {
            DateTime toDate = new DateTime(TO_TIMESTAMP * 1000).plusDays(1);
            DateTime fromDate = new DateTime(FROM_TIMESTAMP * 1000);
            int initNumOfDays = Days.daysBetween(fromDate, toDate).getDays();
            if (this.initCryptoDataset == null) {
                List<String> cryptos = Arrays.stream(Crypto.values()).map(Crypto::name).collect(Collectors.toList());
                this.initCryptoDataset = queryHandler.getCryptoPairs(cryptos, initNumOfDays, TO_TIMESTAMP, false);
            }
            switch (theTable) {
                case "input.daily_changes" :
                    List<String> dates = queryHandler.getDatesBetween(FROM_TIMESTAMP, TO_TIMESTAMP + 86400000);
                    queryHandler.createDailyChangeTable(initCryptoDataset);
                    queryHandler.insertDailyChangeQuery(initCryptoDataset, dates);
                    break;
                case "input.crypto" :
                    queryHandler.createCryptoTable();
                    queryHandler.insertCryptoQuery(initCryptoDataset);
                    break;
                default:
                    break;
            }
        }
        if (theTable.equals("input.daily_changes") || theTable.equals("input.crypto")) {
            log.info("Table {} is initiated", theTable);
        } else {
            log.error("Cannot initiate table {}", theTable);
        }
    }

}
