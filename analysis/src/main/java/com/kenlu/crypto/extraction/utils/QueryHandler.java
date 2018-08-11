package com.kenlu.crypto.extraction.utils;

import com.kenlu.crypto.domain.Crypto;
import com.kenlu.crypto.domain.OHLCV;
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

    public void insertCryptoOHLCVQuery(List<OHLCV> ohlcvList) {
        log.info("Inserting data for table input.crypto_ohlcv...");
        ohlcvList.stream()
                .sorted(Comparator.comparing(OHLCV::getDate))
                .map(OHLCV::getDate)
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

                    handleOHLCVInsert(ohlcvList, date, insertValues);

                    insertSqlStatement = String.format(
                            "INSERT INTO input.crypto_ohlcv VALUES (%s)",
                            insertValues.substring(0, insertValues.lastIndexOf(","))
                    );

                    this.jdbcTemplate.update(insertSqlStatement);
                    log.info("OHLCVs of cryptos on {} are inserted", stringDate);
                });
    }

    public void insertStockOHLCVQuery(List<OHLCV> ohlcvList) {
        log.info("Inserting data for table input.stock_ohlcv...");
        ohlcvList.stream()
                .sorted(Comparator.comparing(OHLCV::getDate))
                .map(OHLCV::getDate)
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

                    handleOHLCVInsert(ohlcvList, date, insertValues);

                    insertSqlStatement = String.format(
                            "INSERT INTO input.stock_ohlcv VALUES (%s)",
                            insertValues.substring(0, insertValues.lastIndexOf(","))
                    );

                    this.jdbcTemplate.update(insertSqlStatement);
                    log.info("OHLCVs of stocks on {} are inserted", stringDate);
                });
    }

    private void handleOHLCVInsert(List<OHLCV> ohlcvList, Date date, StringBuilder insertValues) {
        ohlcvList.stream()
                .filter(ohlcv -> ohlcv.getDate().equals(date))
                .sorted(Comparator.comparing(ohlcv -> ohlcv.getProduct().toString()))
                .forEach(ohlcv -> {
                    String open = ohlcv.getOpen().toString();
                    String high = ohlcv.getHigh().toString();
                    String low = ohlcv.getLow().toString();
                    String close = ohlcv.getClose().toString();
                    String volume = ohlcv.getVolume().toString();
                    List<String> values = new ArrayList<>();
                    values.add(open);
                    values.add(high);
                    values.add(low);
                    values.add(close);
                    values.add(volume);

                    values.forEach(s ->
                            insertValues.append("'")
                                    .append(s)
                                    .append("'")
                                    .append(", ")
                    );
                });
    }

    public void insertCryptoDailyChangeQuery(List<OHLCV> ohlcvList) {
        log.info("Inserting data for table input.crypto_daily_changes...");
        ohlcvList.stream()
                .sorted(Comparator.comparing(OHLCV::getDate))
                .map(OHLCV::getDate)
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

                    handleDailyChangeInsert(ohlcvList, date, insertValues);

                    insertSqlStatement = String.format(
                            "INSERT INTO input.crypto_daily_changes VALUES (%s)",
                            insertValues.substring(0, insertValues.lastIndexOf(","))
                    );

                    this.jdbcTemplate.update(insertSqlStatement);
                    log.info("Daily changes of cryptos on {} are inserted", stringDate);
                });
    }

    public void insertStockDailyChangeQuery(List<OHLCV> ohlcvList) {
        log.info("Inserting data for table input.stock_daily_changes...");
        ohlcvList.stream()
                .sorted(Comparator.comparing(OHLCV::getDate))
                .map(OHLCV::getDate)
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

                    handleDailyChangeInsert(ohlcvList, date, insertValues);

                    insertSqlStatement = String.format(
                            "INSERT INTO input.stock_daily_changes VALUES (%s)",
                            insertValues.substring(0, insertValues.lastIndexOf(","))
                    );

                    this.jdbcTemplate.update(insertSqlStatement);
                    log.info("Daily changes of stocks on {} are inserted", stringDate);
                });
    }

    private void handleDailyChangeInsert(List<OHLCV> ohlcvList, Date date, StringBuilder insertValues) {
        ohlcvList.stream()
                .filter(ohlcv -> ohlcv.getDate().equals(date))
                .sorted(Comparator.comparing(ohlcv -> ohlcv.getProduct().toString()))
                .forEach(ohlcv -> {
                    String dailyChange;
                    try {
                        dailyChange = ohlcv.getClose().divide(ohlcv.getOpen(), 15, BigDecimal.ROUND_HALF_UP).toString();
                    } catch (ArithmeticException e) {
                        dailyChange = "NaN";
                    }

                    insertValues.append("'")
                            .append(dailyChange)
                            .append("'")
                            .append(", ");
                });
    }

    public void insertCryptoQuery(List<OHLCV> ohlcvList) {
        log.info("Inserting data for table input.crypto...");
        ohlcvList.stream()
                .sorted(Comparator.comparing(ohlcv -> ohlcv.getProduct().toString()))
                .map(OHLCV::getProduct)
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

    public void insertStockQuery(List<OHLCV> ohlcvList) {
        log.info("Inserting data for table input.stock...");
        ohlcvList.stream()
                .sorted(Comparator.comparing(ohlcv -> ohlcv.getProduct().toString()))
                .map(OHLCV::getProduct)
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

    public void createCryptoOHLCVTable(List<OHLCV> ohlcvList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd");

        log.info("Creating table input.crypto_ohlcv...");

        ohlcvList.stream()
                .sorted(Comparator.comparing(ohlcv -> ohlcv.getProduct().toString()))
                .filter(ohlcv -> f.format(ohlcv.getDate()).equals("2016-01-01"))
                .forEach(ohlcv -> {
                    handleOHLCVCreate(createCols, ohlcv);
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

    public void createStockOHLCVTable(List<OHLCV> ohlcvList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd");

        log.info("Creating table input.stock_ohlcv...");

        ohlcvList.stream()
                .sorted(Comparator.comparing(ohlcv -> ohlcv.getProduct().toString()))
                .filter(ohlcv -> f.format(ohlcv.getDate()).equals("2016-01-04"))
                .forEach(ohlcv -> {
                    handleOHLCVCreate(createCols, ohlcv);
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

    private void handleOHLCVCreate(StringBuilder createCols, OHLCV ohlcv) {
        String product = ohlcv.getProduct().toString();
        String open = product + "_open";
        String high = product + "_high";
        String low = product + "_low";
        String close = product + "_close";
        String volume = product + "_volume";
        List<String> columns = new ArrayList<>();
        columns.add(open);
        columns.add(high);
        columns.add(low);
        columns.add(close);
        columns.add(volume);
        columns.forEach(s ->
                createCols.append("\"")
                        .append(s)
                        .append("\" ")
                        .append("character varying(100) NOT NULL")
                        .append(", ")
        );
    }

    public void createCryptoDailyChangeTable(List<OHLCV> ohlcvList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        log.info("Creating table input.crypto_daily_changes...");

        handleDailyChangeCreate(ohlcvList, createCols);

        createSqlStatement = String.format(
                "CREATE TABLE input.crypto_daily_changes (" +
                        "\"date\" date NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.crypto_daily_changes is created");
    }

    public void createStockDailyChangeTable(List<OHLCV> ohlcvList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        log.info("Creating table input.stock_daily_changes...");

        handleDailyChangeCreate(ohlcvList, createCols);

        createSqlStatement = String.format(
                "CREATE TABLE input.stock_daily_changes (" +
                        "\"date\" date NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.stock_daily_changes is created");
    }

    private void handleDailyChangeCreate(List<OHLCV> ohlcvList, StringBuilder createCols) {
        ohlcvList.stream()
                .sorted(Comparator.comparing(ohlcv -> ohlcv.getProduct().toString()))
                .map(OHLCV::getProduct)
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
