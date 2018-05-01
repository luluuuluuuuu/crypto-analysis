package com.kenlu.crypto.extraction.utils;

import com.kenlu.crypto.domain.Crypto;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class QueryHandler {

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private DataExtractor dataExtractor;

    public void insertDailyChangeQuery(Map<Crypto, List<String>> cryptoDataset, List<String> dates) {
        List<List<String>> dataArray = new ArrayList<>(cryptoDataset.values());

        log.info("Inserting data for table daily_changes...");
        for (int i = 0; i < dataArray.get(0).size(); i++) {
            StringBuilder insertValues = new StringBuilder();
            String insertSqlStatement;

            insertValues.append("'")
                    .append(dates.get(i))
                    .append("'")
                    .append(", ");

            for (int j = 0; j < dataArray.size(); j++) {
                String value = dataArray.get(j).get(i);

                insertValues.append("'")
                        .append(value)
                        .append("'")
                        .append(", ");
            }

            insertSqlStatement = String.format(
                    "INSERT INTO input.daily_changes VALUES (%s)",
                    insertValues.substring(0, insertValues.lastIndexOf(","))
            );

            this.jdbcTemplate.update(insertSqlStatement);
            log.info("Daily changes on {} is inserted", dates.get(i));
        }
    }

    public void insertCryptoQuery(Map<Crypto, List<String>> cryptoDataset) {
        log.info("Inserting data for table crypto...");
        cryptoDataset.entrySet().stream()
                .forEach(pairs -> {
                            StringBuilder insertValue = new StringBuilder();
                            String insertSqlStatement;

                            insertValue.append("'")
                                    .append(pairs.getKey().name())
                                    .append("'");

                            insertSqlStatement = String.format(
                                    "INSERT INTO input.crypto VALUES (%s)",
                                    insertValue
                            );

                            this.jdbcTemplate.update(insertSqlStatement);
                            log.info("Crypto {} is inserted", pairs.getKey().name());
                        }
                );
    }

    public Map<Crypto, List<String>> getCryptoPairs(List<String> cryptos, int numOfDays, long toTimestamp, boolean isUpdate) throws Exception {
        Map<Crypto, List<String>> cryptoPairs = new HashMap<>();

        for (int i = 0; i < cryptos.size(); i++) {
            List<String> values = new ArrayList<>(this.dataExtractor
                    .getDailyChanges(Crypto.valueOf(cryptos.get(i)), numOfDays, toTimestamp, isUpdate)
                    .values());

            if (isValidCrypto(values)) {
                cryptoPairs.put(Crypto.valueOf(cryptos.get(i)), values);
            }
        }
        return cryptoPairs;
    }

    public void createDailyChangeTable(Map<Crypto, List<String>> cryptoDataset) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        log.info("Creating table daily_changes...");

        cryptoDataset.entrySet().stream()
                .forEach(pairs ->
                        createCols.append("\"")
                                .append(pairs.getKey().name())
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
        log.info("Table daily_changes is created");
    }

    public void createCryptoTable() {
        String createSqlStatement;

        log.info("Creating table crypto...");

        createSqlStatement =
                "CREATE TABLE input.crypto (\"symbol\" character varying(30) NOT NULL PRIMARY KEY)";

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table crypto is created");
    }

    public Date getLastDateFromDailyChanges() {
        String selectSqlStatement =
                "SELECT date FROM input.daily_changes ORDER BY date DESC LIMIT 1";

        return (Date) this.jdbcTemplate.queryForList(selectSqlStatement).get(0).get("date");
    }

    public List<String> getCryptos() {
        String selectSqlStatement =
                "SELECT symbol FROM input.crypto";

        return this.jdbcTemplate.queryForList(selectSqlStatement).stream()
                .map(x -> (String) x.get("symbol"))
                .collect(Collectors.toList());
    }

    public List<String> getDatesBetween(long fromTimestamp, long toTimestamp) {
        DateTime endDate = new DateTime(toTimestamp * 1000);
        DateTime startDate = new DateTime(fromTimestamp * 1000);
        List<String> dates = new ArrayList<>();

        Stream.iterate(startDate, date -> date.plusDays(1))
                .limit(Days.daysBetween(startDate, endDate)
                        .getDays())
                .forEach(date ->
                        dates.add(DateTimeFormat.forPattern("yyyy-MM-dd").print(date))
                );
        return dates;
    }

    private boolean isValidCrypto(List<String> list) {
        return !list.contains("NaN")
                && !list.contains(null)
                && !list.contains("")
                && !list.isEmpty();
    }

}
