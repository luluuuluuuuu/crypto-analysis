package com.kenlu.crypto.extraction.serviceimpl;

import com.kenlu.crypto.analysis.domain.Crypto;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

@Slf4j
@Component
public class ExtractionServiceImpl implements CommandLineRunner {

    private static final int NUM_OF_DAYS = 365;
    private static final long TO_TIMESTAMP = System.currentTimeMillis() / 1000;

    private Map<Crypto, List<String>> cryptoDataset;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private DataFactory dataFactory;

    @PostConstruct
    private void init() throws Exception {
        try {
            this.cryptoDataset = this.getCryptoPairs();
            dropTable("daily_changes");
            this.createDailyChangeTable();
            dropTable("crypto");
            this.createCryptoTable();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(String... args) {
        this.insertDailyChangeQuery();
        this.insertCryptoQuery();
    }

    private void dropTable(String table) throws SQLException {
        boolean isDailyChangeTableExist = this.jdbcTemplate
                .getDataSource()
                .getConnection()
                .getMetaData()
                .getTables(null, null, table, null)
                .next();
        if (isDailyChangeTableExist) {
            String dropStatement = "DROP TABLE public.daily_changes";
            this.jdbcTemplate.execute(dropStatement);
        }
    }

    private void insertDailyChangeQuery() {
        List<String> dates = this.getDates();
        List<List<String>> dataArray = new ArrayList<>(cryptoDataset.values());

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
                    "INSERT INTO public.daily_changes VALUES (%s)",
                    insertValues.substring(0, insertValues.lastIndexOf(","))
            );

            this.jdbcTemplate.update(insertSqlStatement);
        }
    }

    private void insertCryptoQuery() {
        cryptoDataset.entrySet().stream()
                .forEach(pairs -> {
                        StringBuilder insertValue = new StringBuilder();
                        String insertSqlStatement;

                        insertValue.append("'")
                                .append(pairs.getKey().name())
                                .append("'");

                        insertSqlStatement = String.format(
                                "INSERT INTO public.crypto VALUES (%s)",
                                insertValue
                        );

                        this.jdbcTemplate.update(insertSqlStatement);
                    }
                );
    }

    private Map<Crypto, List<String>> getCryptoPairs() throws Exception {
        Map<Crypto, List<String>> cryptoPairs = new HashMap<>();

        for (int i = 0; i < Crypto.values().length; i++) {
            List<String> values = new ArrayList<>(this.dataFactory
                    .getDailyChanges(Crypto.values()[i], NUM_OF_DAYS, TO_TIMESTAMP)
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

        cryptoDataset.entrySet().stream()
                .forEach(pairs ->
                    createCols.append("\"")
                            .append(pairs.getKey().name())
                            .append("\" ")
                            .append("character varying(30) NOT NULL")
                            .append(", ")
                );

        createSqlStatement = String.format(
                "CREATE TABLE public.daily_changes (" +
                        "\"date\" character varying(30) NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
    }

    private void createCryptoTable() {
        String createSqlStatement;

        createSqlStatement =
                "CREATE TABLE public.crypto (\"symbol\" character varying(30) NOT NULL PRIMARY KEY)";

        this.jdbcTemplate.execute(createSqlStatement);
    }

    private List<String> getDates() {
        DateTime endDate = new DateTime(TO_TIMESTAMP * 1000);
        DateTime startDate = endDate.minusDays(NUM_OF_DAYS);
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
