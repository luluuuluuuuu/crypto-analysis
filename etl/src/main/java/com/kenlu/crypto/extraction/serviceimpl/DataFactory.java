package com.kenlu.crypto.extraction.serviceimpl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.kenlu.crypto.analysis.domain.Crypto;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Future;

@Component
public class DataFactory {

    protected Map<String, String> getDailyChanges(Crypto crypto, int numOfDays, long toTimestamp) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("extraParams", "crypto-analysis");
        params.put("limit", Integer.toString(numOfDays - 1));
        params.put("toTs", Long.toString(toTimestamp));

        Map<String, String> row = new TreeMap<>();

        this.getHistoDay(crypto.name(), "USD", params)
                .get("Data")
                .getAsJsonArray()
                .iterator()
                .forEachRemaining(x -> {
                    JsonObject jsonObject = x.getAsJsonObject();
                    Date date = new Date(jsonObject.get("time").getAsLong() * 1000);
                    DateFormat f = new SimpleDateFormat("yyyy/MM/dd");
                    double open = jsonObject.get("open").getAsDouble();
                    double close = jsonObject.get("close").getAsDouble();
                    String changes = Double.toString(close / open);

                    row.put(f.format(date), changes);
                });

        return row;
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
