package com.kenlu.crypto.extraction.utils;

import com.kenlu.crypto.domain.Crypto;
import com.kenlu.crypto.domain.OHLC;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class QueryHandler {

    private JdbcTemplate jdbcTemplate;

    public QueryHandler(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertCryptoOHLCVQuery(List<OHLC> OHLCList) {
        log.info("Inserting data for table input.crypto_ohlcv...");
        OHLCList.stream()
                .sorted(Comparator.comparing(OHLC::getDate))
                .map(OHLC::getDate)
                .distinct()
                .forEach(date -> {
                    StringBuilder insertValues = new StringBuilder();
                    String insertSqlStatement;
                    DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                    String stringDate = f.format(date);

                    insertValues.append("'")
                            .append(stringDate)
                            .append("'")
                            .append(", ");

                    handleOHLCVInsert(OHLCList, date, insertValues);

                    insertSqlStatement = String.format(
                            "INSERT INTO input.crypto_ohlcv VALUES (%s)",
                            insertValues.substring(0, insertValues.lastIndexOf(","))
                    );

                    this.jdbcTemplate.update(insertSqlStatement);
                    log.info("OHLCVs of cryptos on {} are inserted", stringDate);
                });
    }

    public void insertStockOHLCVQuery(List<OHLC> OHLCList) {
        log.info("Inserting data for table input.stock_ohlcv...");
        OHLCList.stream()
                .sorted(Comparator.comparing(OHLC::getDate))
                .map(OHLC::getDate)
                .distinct()
                .forEach(date -> {
                    StringBuilder insertValues = new StringBuilder();
                    String insertSqlStatement;
                    DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                    String stringDate = f.format(date);

                    insertValues.append("'")
                            .append(stringDate)
                            .append("'")
                            .append(", ");

                    handleOHLCVInsert(OHLCList, date, insertValues);

                    insertSqlStatement = String.format(
                            "INSERT INTO input.stock_ohlcv VALUES (%s)",
                            insertValues.substring(0, insertValues.lastIndexOf(","))
                    );

                    this.jdbcTemplate.update(insertSqlStatement);
                    log.info("OHLCVs of stocks on {} are inserted", stringDate);
                });
    }

    private void handleOHLCVInsert(List<OHLC> OHLCList, Date date, StringBuilder insertValues) {
        OHLCList.stream()
                .filter(OHLC -> OHLC.getDate().equals(date))
                .sorted(Comparator.comparing(OHLC -> OHLC.getProduct().toString()))
                .forEach(OHLC -> {
                    String open = OHLC.getOpen().toString();
                    String high = OHLC.getHigh().toString();
                    String low = OHLC.getLow().toString();
                    String close = OHLC.getClose().toString();
                    List<String> values = new ArrayList<>();
                    values.add(open);
                    values.add(high);
                    values.add(low);
                    values.add(close);

                    values.forEach(s ->
                            insertValues.append("'")
                                    .append(s)
                                    .append("'")
                                    .append(", ")
                    );
                });
    }

    public void insertCryptoDailyChangeQuery(List<OHLC> OHLCList) {
        log.info("Inserting data for table input.crypto_daily_changes...");
        OHLCList.stream()
                .sorted(Comparator.comparing(OHLC::getDate))
                .map(OHLC::getDate)
                .distinct()
                .forEach(date -> {
                    StringBuilder insertValues = new StringBuilder();
                    String insertSqlStatement;
                    DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                    String stringDate = f.format(date);

                    insertValues.append("'")
                            .append(stringDate)
                            .append("'")
                            .append(", ");

                    handleDailyChangeInsert(OHLCList, date, insertValues);

                    insertSqlStatement = String.format(
                            "INSERT INTO input.crypto_daily_changes VALUES (%s)",
                            insertValues.substring(0, insertValues.lastIndexOf(","))
                    );

                    this.jdbcTemplate.update(insertSqlStatement);
                    log.info("Daily changes of cryptos on {} are inserted", stringDate);
                });
    }

    public void insertStockDailyChangeQuery(List<OHLC> OHLCList) {
        log.info("Inserting data for table input.stock_daily_changes...");
        OHLCList.stream()
                .sorted(Comparator.comparing(OHLC::getDate))
                .map(OHLC::getDate)
                .distinct()
                .forEach(date -> {
                    StringBuilder insertValues = new StringBuilder();
                    String insertSqlStatement;
                    DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                    String stringDate = f.format(date);

                    insertValues.append("'")
                            .append(stringDate)
                            .append("'")
                            .append(", ");

                    handleDailyChangeInsert(OHLCList, date, insertValues);

                    insertSqlStatement = String.format(
                            "INSERT INTO input.stock_daily_changes VALUES (%s)",
                            insertValues.substring(0, insertValues.lastIndexOf(","))
                    );

                    this.jdbcTemplate.update(insertSqlStatement);
                    log.info("Daily changes of stocks on {} are inserted", stringDate);
                });
    }

    private void handleDailyChangeInsert(List<OHLC> OHLCList, Date date, StringBuilder insertValues) {
        OHLCList.stream()
                .filter(OHLC -> OHLC.getDate().equals(date))
                .sorted(Comparator.comparing(OHLC -> OHLC.getProduct().toString()))
                .forEach(OHLC -> {
                    String dailyChange;
                    try {
                        dailyChange = OHLC.getClose().divide(OHLC.getOpen(), 15, BigDecimal.ROUND_HALF_UP).toString();
                    } catch (ArithmeticException e) {
                        dailyChange = "NaN";
                    }

                    insertValues.append("'")
                            .append(dailyChange)
                            .append("'")
                            .append(", ");
                });
    }

    public void insertCryptoQuery(List<OHLC> OHLCList) {
        log.info("Inserting data for table input.crypto...");
        OHLCList.stream()
                .sorted(Comparator.comparing(OHLC -> OHLC.getProduct().toString()))
                .map(OHLC::getProduct)
                .distinct()
                .forEach(crypto -> {
                            StringBuilder insertValue = new StringBuilder();
                            String insertSqlStatement;

                            insertValue.append("'")
                                    .append(crypto.toString())
                                    .append("'");

                            insertSqlStatement = String.format(
                                    "INSERT INTO input.crypto VALUES (%s)",
                                    insertValue
                            );

                            this.jdbcTemplate.update(insertSqlStatement);
                            log.info("Crypto {} is inserted", crypto.toString());
                        }
                );
    }

    public void insertStockQuery(List<OHLC> OHLCList) {
        log.info("Inserting data for table input.stock...");
        OHLCList.stream()
                .sorted(Comparator.comparing(OHLC -> OHLC.getProduct().toString()))
                .map(OHLC::getProduct)
                .distinct()
                .forEach(stock -> {
                            StringBuilder insertValue = new StringBuilder();
                            String insertSqlStatement;

                            insertValue.append("'")
                                    .append(stock.toString())
                                    .append("'");

                            insertSqlStatement = String.format(
                                    "INSERT INTO input.stock VALUES (%s)",
                                    insertValue
                            );

                            this.jdbcTemplate.update(insertSqlStatement);
                            log.info("Stock {} is inserted", stock.toString());
                        }
                );
    }

    public void createCryptoOHLCVTable(List<OHLC> OHLCList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd");

        log.info("Creating table input.crypto_ohlcv...");

        OHLCList.stream()
                .sorted(Comparator.comparing(OHLC -> OHLC.getProduct().toString()))
                .filter(OHLC -> f.format(OHLC.getDate()).equals("2016-01-01"))
                .forEach(OHLC -> {
                    handleOHLCVCreate(createCols, OHLC);
                });

        createSqlStatement = String.format(
                "CREATE TABLE input.crypto_ohlcv (" +
                        "\"date\" date NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.crypto_ohlcv is created");
    }

    public void createStockOHLCVTable(List<OHLC> OHLCList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd");

        log.info("Creating table input.stock_ohlcv...");

        OHLCList.stream()
                .sorted(Comparator.comparing(OHLC -> OHLC.getProduct().toString()))
                .filter(OHLC -> f.format(OHLC.getDate()).equals("2016-01-04"))
                .forEach(OHLC -> {
                    handleOHLCVCreate(createCols, OHLC);
                });

        createSqlStatement = String.format(
                "CREATE TABLE input.stock_ohlcv (" +
                        "\"date\" date NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.stock_ohlcv is created");
    }

    private void handleOHLCVCreate(StringBuilder createCols, OHLC OHLC) {
        String product = OHLC.getProduct().toString();
        String open = product + "_open";
        String high = product + "_high";
        String low = product + "_low";
        String close = product + "_close";
        List<String> columns = new ArrayList<>();
        columns.add(open);
        columns.add(high);
        columns.add(low);
        columns.add(close);
        columns.forEach(s ->
                createCols.append("\"")
                        .append(s)
                        .append("\" ")
                        .append("character varying(100) NOT NULL")
                        .append(", ")
        );
    }

    public void createCryptoDailyChangeTable(List<OHLC> OHLCList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        log.info("Creating table input.crypto_daily_changes...");

        handleDailyChangeCreate(OHLCList, createCols);

        createSqlStatement = String.format(
                "CREATE TABLE input.crypto_daily_changes (" +
                        "\"date\" date NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.crypto_daily_changes is created");
    }

    public void createStockDailyChangeTable(List<OHLC> OHLCList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        log.info("Creating table input.stock_daily_changes...");

        handleDailyChangeCreate(OHLCList, createCols);

        createSqlStatement = String.format(
                "CREATE TABLE input.stock_daily_changes (" +
                        "\"date\" date NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.stock_daily_changes is created");
    }

    private void handleDailyChangeCreate(List<OHLC> OHLCList, StringBuilder createCols) {
        OHLCList.stream()
                .sorted(Comparator.comparing(OHLC -> OHLC.getProduct().toString()))
                .map(OHLC::getProduct)
                .distinct()
                .forEach(product ->
                        createCols.append("\"")
                                .append(product.toString())
                                .append("\" ")
                                .append("character varying(30) NOT NULL")
                                .append(", ")
                );
    }

    public void createCryptoTable() {
        String createSqlStatement;

        log.info("Creating table input.crypto...");

        createSqlStatement =
                "CREATE TABLE input.crypto (\"symbol\" character varying(30) NOT NULL PRIMARY KEY)";

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.crypto is created");
    }

    public void createStockTable() {
        String createSqlStatement;

        log.info("Creating table input.stock...");

        createSqlStatement =
                "CREATE TABLE input.stock (\"symbol\" character varying(30) NOT NULL PRIMARY KEY)";

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.stock is created");
    }

    public Date getLastDateFromDailyChanges() {
        String selectSqlStatement =
                "SELECT date FROM input.crypto_daily_changes ORDER BY date DESC LIMIT 1";

        return (Date) this.jdbcTemplate.queryForList(selectSqlStatement).get(0).get("date");
    }

    public List<Crypto> getCryptos() {
        String selectSqlStatement =
                "SELECT symbol FROM input.crypto ORDER BY symbol ASC";

        return this.jdbcTemplate.queryForList(selectSqlStatement).stream()
                .map(x -> Crypto.valueOf((String) x.get("symbol")))
                .collect(Collectors.toList());
    }

    public List<String> getDatesBetween(long fromTimestamp, long toTimestamp) {
        DateTime endDate = new DateTime(toTimestamp);
        DateTime startDate = new DateTime(fromTimestamp);
        List<String> dates = new ArrayList<>();

        Stream.iterate(startDate, date -> date.plusDays(1))
                .limit(Days.daysBetween(startDate, endDate)
                        .getDays())
                .forEach(date ->
                        dates.add(DateTimeFormat.forPattern("yyyy-MM-dd").print(date))
                );
        return dates;
    }

    public void dropTable(String schema, String table) {
        log.info("Dropping table {}...", schema + "." + table);
        String dropStatement = String.format("DROP TABLE IF EXISTS %s", schema + "." + table);
        this.jdbcTemplate.execute(dropStatement);
    }

}
