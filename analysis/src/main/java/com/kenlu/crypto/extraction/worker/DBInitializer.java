package com.kenlu.crypto.extraction.worker;

import com.kenlu.crypto.domain.Crypto;
import com.kenlu.crypto.domain.OHLCV;
import com.kenlu.crypto.extraction.utils.DataExtractor;
import com.kenlu.crypto.extraction.utils.QueryHandler;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.*;

@Slf4j
@Component
public class DBInitializer {

    private static final long TO_TIMESTAMP = System.currentTimeMillis() / 1000;
    private static final long FROM_TIMESTAMP = 1483228800;

    private List<OHLCV> initOhlcvList;
    private JdbcTemplate jdbcTemplate;
    private QueryHandler queryHandler;
    private DataExtractor dataExtractor;

    public DBInitializer(JdbcTemplate jdbcTemplate, QueryHandler queryHandler, DataExtractor dataExtractor) {
        this.initOhlcvList = new ArrayList<>();
        this.jdbcTemplate = jdbcTemplate;
        this.queryHandler = queryHandler;
        this.dataExtractor = dataExtractor;
    }

    public void run() throws Exception {
        log.info("Initiating tables...");
        try {
            initTableIfNotExist("input", "crypto");
            initTableIfNotExist("input", "ohlcv");
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
            if (this.initOhlcvList.size() == 0) {
                Arrays.stream(Crypto.values())
                        .forEach(crypto -> {
                            try {
                                List<OHLCV> tmpList = dataExtractor.getDailyOHLCVs(crypto, initNumOfDays, TO_TIMESTAMP, false);
                                initOhlcvList.addAll(tmpList);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
            }
            switch (theTable) {
                case "input.ohlcv" :
                    queryHandler.createOHLCVTable(initOhlcvList);
                    queryHandler.insertOHLCVQuery(initOhlcvList);
                    break;
                case "input.daily_changes" :
                    queryHandler.createDailyChangeTable(initOhlcvList);
                    queryHandler.insertDailyChangeQuery(initOhlcvList);
                    break;
                case "input.crypto" :
                    queryHandler.createCryptoTable();
                    queryHandler.insertCryptoQuery(initOhlcvList);
                    break;
                default:
                    break;
            }
        }
        if (theTable.equals("input.ohlcv") || theTable.equals("input.daily_changes") || theTable.equals("input.crypto")) {
            log.info("Table {} is initiated", theTable);
        } else {
            log.error("Cannot initiate table {}", theTable);
        }
    }

}
