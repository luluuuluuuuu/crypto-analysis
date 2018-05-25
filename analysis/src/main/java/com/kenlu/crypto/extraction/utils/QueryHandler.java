package com.kenlu.crypto.extraction.utils;

import com.kenlu.crypto.domain.Crypto;
import com.kenlu.crypto.domain.DailyOHLCV;
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

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public void insertDailyChangeQuery(List<DailyOHLCV> ohlcvList) {
        log.info("Inserting data for table input.daily_changes...");
        ohlcvList.stream()
                .sorted(Comparator.comparing(DailyOHLCV::getDate))
                .map(DailyOHLCV::getDate)
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
                            .filter(dailyOHLCV -> dailyOHLCV.getDate().equals(date))
                            .sorted((o1, o2) -> o1.getCrypto().name().compareTo(o2.getCrypto().name()))
                            .forEach(dailyOHLCV -> {
                                String dailyChange;
                                try {
                                    dailyChange = dailyOHLCV.getClose().divide(dailyOHLCV.getOpen(), 15, BigDecimal.ROUND_HALF_UP).toString();
                                } catch (ArithmeticException e) {
                                    dailyChange = "NaN";
                                    e.printStackTrace();
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

    public void insertCryptoQuery(List<DailyOHLCV> ohlcvList) {
        log.info("Inserting data for table input.crypto...");
        ohlcvList.stream()
                .sorted((o1, o2) -> o1.getCrypto().name().compareTo(o2.getCrypto().name()))
                .map(DailyOHLCV::getCrypto)
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

    public void createDailyChangeTable(List<DailyOHLCV> ohlcvList) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        log.info("Creating table input.daily_changes...");

        ohlcvList.stream()
                .sorted((o1, o2) -> o1.getCrypto().name().compareTo(o2.getCrypto().name()))
                .map(DailyOHLCV::getCrypto)
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
