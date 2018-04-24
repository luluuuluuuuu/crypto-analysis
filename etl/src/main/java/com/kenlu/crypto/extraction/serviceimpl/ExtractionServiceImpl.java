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
    private static final long TO_TIMESTAMP = 1524355200;

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private DataFactory dataFactory;

    @PostConstruct
    private void init() {
        try {
            boolean isTableExist = jdbcTemplate
                    .getDataSource()
                    .getConnection()
                    .getMetaData()
                    .getTables(null, null, "daily_changes", null)
                    .next();
            if (!isTableExist) {
                createDailyChangeTable();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(String... args) throws Exception {
        insertDailyChangeQuery();
    }

    private void insertDailyChangeQuery() throws Exception {
        StringBuilder insertCols = new StringBuilder();
        String[][] dataArray = new String[NUM_OF_DAYS][Crypto.values().length];
        List<String> dates = getDates();

        for (int i = 0; i < Crypto.values().length; i++) {
            insertCols.append("\"")
                    .append(Crypto.values()[i])
                    .append("\"")
                    .append(", ");

            String[] values = dataFactory
                    .getDailyChanges(Crypto.values()[i].name(), NUM_OF_DAYS, TO_TIMESTAMP)
                    .values()
                    .stream()
                    .map(x -> Double.toString(x))
                    .toArray(String[]::new);

            for (int j = 0; j < values.length; j++) {
                dataArray[j][i] = values[j];
            }
        }

        for (int i = 0; i < dataArray.length; i++) {
            StringBuilder insertValues = new StringBuilder();
            String insertSqlStatement;

            insertValues.append("'")
                    .append(dates.get(i))
                    .append("'")
                    .append(", ");

            for (int j = 0; j < dataArray[i].length; j++) {
                insertValues.append("'")
                        .append(dataArray[i][j])
                        .append("'")
                        .append(", ");
            }

            insertSqlStatement = String.format(
                    "INSERT INTO public.daily_changes (\"date\", %s) VALUES (%s)",
                    insertCols.substring(0, insertCols.lastIndexOf(",")),
                    insertValues.substring(0, insertValues.lastIndexOf(","))
            );
            jdbcTemplate.update(insertSqlStatement);
        }
    }

    private void createDailyChangeTable() {
        String createSqlStatement;
        StringBuilder createCols = new StringBuilder();

        Arrays.stream(Crypto.values())
                .forEach(crypto ->
                        createCols.append("\"")
                                .append(crypto.name())
                                .append("\" ")
                                .append("numeric")
                                .append(", ")
                );

        createSqlStatement = String.format(
                "CREATE TABLE public.daily_changes (" +
                        "\"id\" serial NOT NULL, " +
                        "\"date\" character varying(30), " +
                        "%s)",
                createCols.substring(0, createCols.lastIndexOf(","))
        );

        jdbcTemplate.execute(createSqlStatement);
    }

    private List<String> getDates() {
        DateTime endDate = new DateTime(TO_TIMESTAMP * 1000);
        DateTime startDate = endDate.minusDays(NUM_OF_DAYS);
        List<String> dates = new ArrayList<>();

        Stream.iterate(startDate, date -> date.plusDays(1))
                .limit(Days.daysBetween(startDate, endDate)
                        .getDays())
                .forEach(date -> {
                    dates.add(DateTimeFormat.forPattern("yyyy/MM/dd").print(date));
                });
        return dates;
    }

}
