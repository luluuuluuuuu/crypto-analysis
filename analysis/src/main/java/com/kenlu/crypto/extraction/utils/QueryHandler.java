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
import java.sql.SQLException;
import java.util.*;
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

        log.info("Inserting data for table input.daily_changes...");
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
        log.info("Inserting data for table input.crypto...");
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

    public void insertCorrelationQuery(double[][] correlationMatrix) {
        List<Crypto> cryptos = this.getCryptos();

        log.info("Inserting data for table output.correlation...");
        for (int i = 0; i < correlationMatrix.length; i++) {
            StringBuilder insertValues = new StringBuilder();
            String insertSqlStatement;

            insertValues.append("'")
                    .append(cryptos.get(i).name())
                    .append("'")
                    .append(", ");
            for (int j = 0; j < correlationMatrix[i].length; j++) {
                insertValues.append("'")
                        .append(Double.toString(correlationMatrix[i][j]))
                        .append("'")
                        .append(", ");
            }

            insertSqlStatement = String.format(
                    "INSERT INTO output.correlation VALUES (%s)",
                    insertValues.substring(0, insertValues.lastIndexOf(","))
            );

            this.jdbcTemplate.update(insertSqlStatement);
            log.info("Correlations for {} are inserted", cryptos.get(i));
        }
    }

    public Map<Crypto, List<String>> getCryptoPairs(List<Crypto> cryptos, int numOfDays, long toTimestamp, boolean isUpdate) throws Exception {
        Map<Crypto, List<String>> cryptoPairs = new TreeMap<>(Comparator.comparing(Crypto::name));

        for (int i = 0; i < cryptos.size(); i++) {
            List<String> values = new ArrayList<>(this.dataExtractor
                    .getDailyChanges(cryptos.get(i), numOfDays, toTimestamp, isUpdate)
                    .values());

            if (isValidCrypto(values)) {
                cryptoPairs.put(cryptos.get(i), values);
            }
        }

        return cryptoPairs;
    }

    public void createDailyChangeTable(Map<Crypto, List<String>> cryptoDataset) {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        log.info("Creating table input.daily_changes...");

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

    public void createCorrelationTable() {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        log.info("Creating table output.correlation...");

        this.getCryptos().stream()
                .forEach(x -> {
                    createCols.append("\"")
                            .append(x.name())
                            .append("\" ")
                            .append("character varying(30) NOT NULL")
                            .append(", ");
                });

        createSqlStatement = String.format(
                "CREATE TABLE output.correlation (" +
                        "\"crypto\" character varying(30) NOT NULL PRIMARY KEY, " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        this.jdbcTemplate.execute(createSqlStatement);
        log.info("Table output.correlation is created");
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

    public void dropTable(String schema, String table) {

        try {
            boolean isTableExist = this.jdbcTemplate
                    .getDataSource()
                    .getConnection()
                    .getMetaData()
                    .getTables(null, schema, table, null)
                    .next();
            if (isTableExist) {
                log.info("Dropping table {}...", schema + "." + table);
                String dropStatement = String.format("DROP TABLE %s", schema + "." + table);
                this.jdbcTemplate.execute(dropStatement);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
