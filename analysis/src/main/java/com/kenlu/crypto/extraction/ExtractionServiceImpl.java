package com.kenlu.crypto.extraction;

import com.kenlu.crypto.domain.Crypto;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
@Service
@Order(1)
public class ExtractionServiceImpl implements CommandLineRunner {

    private static final long TO_TIMESTAMP = System.currentTimeMillis() / 1000;
    private static final long FROM_TIMESTAMP = 1483228800;

    private Map<Crypto, List<String>> initCryptoDataset;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private DataFactory dataFactory;

    @Override
    public void run(String... args) throws Exception {
        initTables();
        // TODO Insert daily data every day
    }

    private void initTables() throws Exception {
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
                this.initCryptoDataset = this.getCryptoPairs(initNumOfDays, TO_TIMESTAMP);
            }
            switch (table) {
                case "daily_changes" :
                    createDailyChangeTable();
                    insertDailyChangeQuery();
                    break;
                case "crypto" :
                    createCryptoTable();
                    insertCryptoQuery();
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

    private void insertDailyChangeQuery() {
        List<String> dates = this.getDates();
        List<List<String>> dataArray = new ArrayList<>(initCryptoDataset.values());

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

    private void insertCryptoQuery() {
        log.info("Inserting data for table crypto...");
        initCryptoDataset.entrySet().stream()
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

    private Map<Crypto, List<String>> getCryptoPairs(int numOfDays, long toTimestamp) throws Exception {
        Map<Crypto, List<String>> cryptoPairs = new HashMap<>();

        for (int i = 0; i < Crypto.values().length; i++) {
            List<String> values = new ArrayList<>(this.dataFactory
                    .getDailyChanges(Crypto.values()[i], numOfDays, toTimestamp)
                    .values());

            if (isValid(values)) {
                cryptoPairs.put(Crypto.values()[i], values);
            }
        }
        return cryptoPairs;
    }

    private void createDailyChangeTable() {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        log.info("Creating table daily_changes...");

        initCryptoDataset.entrySet().stream()
                .forEach(pairs ->
                    createCols.append("\"")
                            .append(pairs.getKey().name())
                            .append("\" ")
                            .append("character varying(30) NOT NULL")
                            .append(", ")
                );

        createSqlStatement = String.format(
                "CREATE TABLE input.daily_changes (" +
                        "\"date\" character varying(30) NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table daily_changes is created");
    }

    private void createCryptoTable() {
        String createSqlStatement;

        log.info("Creating table crypto...");

        createSqlStatement =
                "CREATE TABLE input.crypto (\"symbol\" character varying(30) NOT NULL PRIMARY KEY)";

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table crypto is created");
    }

    private List<String> getDates() {
        DateTime endDate = new DateTime(TO_TIMESTAMP * 1000);
        DateTime startDate = new DateTime(FROM_TIMESTAMP * 1000);
        List<String> dates = new ArrayList<>();

        Stream.iterate(startDate, date -> date.plusDays(1))
                .limit(Days.daysBetween(startDate, endDate)
                        .getDays())
                .forEach(date ->
                    dates.add(DateTimeFormat.forPattern("yyyy/MM/dd").print(date))
                );
        return dates;
    }

    private boolean isValid(List<String> list) {
        return !list.contains("NaN")
                && !list.contains(null)
                && !list.contains("")
                && !list.isEmpty();
    }

}
