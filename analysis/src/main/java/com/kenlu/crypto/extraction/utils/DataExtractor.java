package com.kenlu.crypto.extraction.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.kenlu.crypto.domain.Crypto;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Component
public class DataExtractor {

    @Autowired
    private QueryHandler queryHandler;

    public Map<String, String> getDailyChanges(Crypto crypto, int numOfDays, long toTimestamp, boolean isUpdate) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("extraParams", "crypto-analysis");
        params.put("limit", Integer.toString(numOfDays - 1));
        params.put("toTs", Long.toString(toTimestamp));

        Map<String, String> result;
        Map<String, String> row = new TreeMap<>();
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd");

        this.getHistoDay(crypto.name(), "USD", params)
                .get("Data")
                .getAsJsonArray()
                .iterator()
                .forEachRemaining(x -> {
                    JsonObject jsonObject = x.getAsJsonObject();
                    Date date = new Date(jsonObject.get("time").getAsLong() * 1000);
                    double open = jsonObject.get("open").getAsDouble();
                    double close = jsonObject.get("close").getAsDouble();
                    String changes = Double.toString(close / open);

                    row.put(f.format(date), changes);
                });
        result = row;

        if (isUpdate) {
            Date lastDate = queryHandler.getLastDateFromDailyChanges();
            result = row.entrySet().stream()
                    .filter(x -> {
                        try {
                            return f.parse(x.getKey()).after(lastDate);
                        } catch(ParseException e) {
                            e.printStackTrace();
                        }
                        return false;
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        return result;
    }

    private JsonObject getHistoDay(String fsym, String tsym, Map<String, Object> optionalParams) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("https://min-api.cryptocompare.com/data/")
                .append("histoday?fsym=")
                .append(fsym)
                .append("&tsym=")
                .append(tsym);
        optionalParams.entrySet()
                .forEach(x ->
                    stringBuilder.append("&")
                            .append(x.getKey())
                            .append("=")
                            .append(x.getValue())
                );
        String requestUrl = stringBuilder.toString();

        return this.getHttpResponse(requestUrl);
    }

    private JsonObject getHttpResponse(String requestUrl) throws InterruptedException, java.util.concurrent.ExecutionException, IOException {
        CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
        client.start();
        HttpGet request = new HttpGet(requestUrl);
        Future<HttpResponse> future = client.execute(request, null);
        HttpResponse response = future.get();
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = (JsonObject) jsonParser
                .parse(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
        client.close();

        return jsonObject;
    }

}
