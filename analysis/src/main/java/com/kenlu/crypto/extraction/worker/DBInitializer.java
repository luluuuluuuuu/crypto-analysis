package com.kenlu.crypto.extraction.worker;

import com.kenlu.crypto.domain.Crypto;
import com.kenlu.crypto.domain.OHLCV;
import com.kenlu.crypto.domain.Stock;
import com.kenlu.crypto.extraction.utils.DataExtractor;
import com.kenlu.crypto.extraction.utils.QueryHandler;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.*;

@Slf4j
@Component
public class DBInitializer {

    private static final long TO_TIMESTAMP = System.currentTimeMillis() / 1000;
    private static final long FROM_TIMESTAMP = 1451606400;

    private List<OHLCV> initCryptoOhlcvList;
    private List<OHLCV> initStockOhlcvList;
    private JdbcTemplate jdbcTemplate;
    private QueryHandler queryHandler;
    private DataExtractor dataExtractor;

    public DBInitializer(JdbcTemplate jdbcTemplate, QueryHandler queryHandler, DataExtractor dataExtractor) {
        this.initCryptoOhlcvList = new ArrayList<>();
        this.initStockOhlcvList = new ArrayList<>();
        this.jdbcTemplate = jdbcTemplate;
        this.queryHandler = queryHandler;
        this.dataExtractor = dataExtractor;
    }

    public void run() throws Exception {
        log.info("Initiating tables...");
        try {
            initTableIfNotExist("input", "crypto");
            initTableIfNotExist("input", "crypto_ohlcv");
            initTableIfNotExist("input", "crypto_daily_changes");
            initTableIfNotExist("input", "stock");
            initTableIfNotExist("input", "stock_ohlcv");
            initTableIfNotExist("input", "stock_daily_changes");
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
            if (initCryptoOhlcvList.size() == 0) {
                Arrays.stream(Crypto.values())
                        .forEach(crypto -> {
                            try {
                                List<OHLCV> tmpList = dataExtractor.getCryptoDailyOHLCVs(crypto, initNumOfDays, TO_TIMESTAMP, false);
                                initCryptoOhlcvList.addAll(tmpList);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
            }
            if (initStockOhlcvList.size() == 0) {
                Arrays.stream(Stock.values())
                        .forEach(stock -> {
                            try {
                                List<OHLCV> tmpList = dataExtractor.getStockDailyOHLCVs(stock, initNumOfDays, TO_TIMESTAMP, false);
                                initStockOhlcvList.addAll(tmpList);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
            }
            switch (theTable) {
                case "input.crypto_ohlcv" :
                    queryHandler.createCryptoOHLCVTable(initCryptoOhlcvList);
                    queryHandler.insertCryptoOHLCVQuery(initCryptoOhlcvList);
                    break;
                case "input.crypto_daily_changes" :
                    queryHandler.createCryptoDailyChangeTable(initCryptoOhlcvList);
                    queryHandler.insertCryptoDailyChangeQuery(initCryptoOhlcvList);
                    break;
                case "input.crypto" :
                    queryHandler.createCryptoTable();
                    queryHandler.insertCryptoQuery(initCryptoOhlcvList);
                    break;
                case "input.stock_ohlcv" :
                    queryHandler.createStockOHLCVTable(initStockOhlcvList);
                    queryHandler.insertStockOHLCVQuery(initStockOhlcvList);
                    break;
                case "input.stock_daily_changes" :
                    queryHandler.createStockDailyChangeTable(initStockOhlcvList);
                    queryHandler.insertStockDailyChangeQuery(initStockOhlcvList);
                    break;
                case "input.stock" :
                    queryHandler.createStockTable();
                    queryHandler.insertStockQuery(initStockOhlcvList);
                    break;
                default:
                    break;
            }
        }
        if (theTable.equals("input.crypto_ohlcv") ||
                theTable.equals("input.crypto_daily_changes") ||
                theTable.equals("input.crypto") ||
                theTable.equals("input.stock_ohlcv") ||
                theTable.equals("input.stock_daily_changes") ||
                theTable.equals("input.stock")) {
            log.info("Table {} is initiated", theTable);
        } else {
            log.error("Cannot initiate table {}", theTable);
        }
    }

}
