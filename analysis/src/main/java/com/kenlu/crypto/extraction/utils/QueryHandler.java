package com.kenlu.crypto.extraction.utils;

import com.kenlu.crypto.domain.Crypto;
import com.kenlu.crypto.domain.OHLCV;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Date;
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

    public void insertOHLCVQuery(List<OHLCV> ohlcvList) {
        log.info("Inserting data for table input.ohlcv...");
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

                    ohlcvList.stream()
                            .filter(ohlcv -> ohlcv.getDate().equals(date))
                            .sorted(Comparator.comparing(ohlcv -> ohlcv.getCrypto().name()))
                            .forEach(ohlcv -> {
                                String open = ohlcv.getOpen().toString();
                                String high = ohlcv.getHigh().toString();
                                String low = ohlcv.getLow().toString();
                                String close = ohlcv.getClose().toString();
                                String volumeFrom = ohlcv.getVolumeFrom().toString();
                                String volumeTo = ohlcv.getVolumeTo().toString();
                                List<String> values = new ArrayList<>();
                                values.add(open);
                                values.add(high);
                                values.add(low);
                                values.add(close);
                                values.add(volumeFrom);
                                values.add(volumeTo);

                                values.forEach(s ->
                                                insertValues.append("'")
                                                        .append(s)
                                                        .append("'")
                                                        .append(", ")
                                );
                            });

                    insertSqlStatement = String.format(
                            "INSERT INTO input.ohlcv VALUES (%s)",
                            insertValues.substring(0, insertValues.lastIndexOf(","))
                    );

                    this.jdbcTemplate.update(insertSqlStatement);
                    log.info("OHLCVs on {} are inserted", stringDate);
                });
    }

    public void insertDailyChangeQuery(List<OHLCV> ohlcvList) {
        log.info("Inserting data for table input.daily_changes...");
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

                    ohlcvList.stream()
                            .filter(ohlcv -> ohlcv.getDate().equals(date))
                            .sorted(Comparator.comparing(ohlcv -> ohlcv.getCrypto().name()))
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

                    insertSqlStatement = String.format(
                            "INSERT INTO input.daily_changes VALUES (%s)",
                            insertValues.substring(0, insertValues.lastIndexOf(","))
                    );

                    this.jdbcTemplate.update(insertSqlStatement);
                    log.info("Daily changes on {} are inserted", stringDate);
                });
    }

    public void insertCryptoQuery(List<OHLCV> ohlcvList) {
        log.info("Inserting data for table input.crypto...");
        ohlcvList.stream()
                .sorted(Comparator.comparing(ohlcv -> ohlcv.getCrypto().name()))
                .map(OHLCV::getCrypto)
                .distinct()
                .forEach(crypto -> {
                            StringBuilder insertValue = new StringBuilder();
                            String insertSqlStatement;

                            insertValue.append("'")
                                    .append(crypto.name())
                                    .append("'");

                            insertSqlStatement = String.format(
                                    "INSERT INTO input.crypto VALUES (%s)",
                                    insertValue
                            );

                            this.jdbcTemplate.update(insertSqlStatement);
                            log.info("Crypto {} is inserted", crypto.name());
                        }
                );
    }

    public void createOHLCVTable(List<OHLCV> ohlcvList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd");

        log.info("Creating table input.ohlcv...");

        ohlcvList.stream()
                .sorted(Comparator.comparing(ohlcv -> ohlcv.getCrypto().name()))
                .filter(ohlcv -> f.format(ohlcv.getDate()).equals("2017-01-01"))
                .forEach(ohlcv -> {
                    String crypto = ohlcv.getCrypto().name();
                    String open = crypto + "_open";
                    String high = crypto + "_high";
                    String low = crypto + "_low";
                    String close = crypto + "_close";
                    String volumeFrom = crypto + "_volumefrom";
                    String volumeTo = crypto + "_volumeto";
                    List<String> columns = new ArrayList<>();
                    columns.add(open);
                    columns.add(high);
                    columns.add(low);
                    columns.add(close);
                    columns.add(volumeFrom);
                    columns.add(volumeTo);
                    columns.forEach(s ->
                                    createCols.append("\"")
                                            .append(s)
                                            .append("\" ")
                                            .append("character varying(100) NOT NULL")
                                            .append(", ")
                    );
                });

        createSqlStatement = String.format(
                "CREATE TABLE input.ohlcv (" +
                        "\"date\" date NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.ohlcv is created");
    }

    public void createDailyChangeTable(List<OHLCV> ohlcvList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        log.info("Creating table input.daily_changes...");

        ohlcvList.stream()
                .sorted(Comparator.comparing(ohlcv -> ohlcv.getCrypto().name()))
                .map(OHLCV::getCrypto)
                .distinct()
                .forEach(crypto ->
                        createCols.append("\"")
                                .append(crypto.name())
                                .append("\" ")
                                .append("character varying(30) NOT NULL")
                                .append(", ")
                );

        createSqlStatement = String.format(
                "CREATE TABLE input.daily_changes (" +
                        "\"date\" date NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.daily_changes is created");
    }

    public void createCryptoTable() {
        String createSqlStatement;

        log.info("Creating table input.crypto...");

        createSqlStatement =
                "CREATE TABLE input.crypto (\"symbol\" character varying(30) NOT NULL PRIMARY KEY)";

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table input.crypto is created");
    }

    public Date getLastDateFromDailyChanges() {
        String selectSqlStatement =
                "SELECT date FROM input.daily_changes ORDER BY date DESC LIMIT 1";

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
