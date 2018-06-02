package com.kenlu.crypto.extraction.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.kenlu.crypto.domain.Crypto;
import com.kenlu.crypto.domain.OHLCV;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Component
public class DataExtractor {

    private QueryHandler queryHandler;

    public DataExtractor(QueryHandler queryHandler) {
        this.queryHandler = queryHandler;
    }

    public List<OHLCV> getDailyOHLCVs(Crypto crypto, int numOfDays, long toTimestamp, boolean isUpdate) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("extraParams", "crypto-analysis");
        params.put("limit", Integer.toString(numOfDays - 1));
        params.put("toTs", Long.toString(toTimestamp));

        List<OHLCV> ohlcvList = new ArrayList<>();
        List<OHLCV> result;

        this.getHistoDay(crypto.name(), "USD", params)
                .get("Data")
                .getAsJsonArray()
                .iterator()
                .forEachRemaining(x -> {
                    JsonObject jsonObject = x.getAsJsonObject();
                    Date date = new Date(jsonObject.get("time").getAsLong() * 1000);
                    BigDecimal open = jsonObject.get("open").getAsBigDecimal();
                    BigDecimal high = jsonObject.get("high").getAsBigDecimal();
                    BigDecimal low = jsonObject.get("low").getAsBigDecimal();
                    BigDecimal close = jsonObject.get("close").getAsBigDecimal();
                    BigDecimal volumeFrom = jsonObject.get("volumefrom").getAsBigDecimal();
                    BigDecimal volumeTo = jsonObject.get("volumeto").getAsBigDecimal();
                    OHLCV ohlcv = new OHLCV();

                    ohlcv.setCrypto(crypto);
                    ohlcv.setDate(date);
                    ohlcv.setOpen(open);
                    ohlcv.setHigh(high);
                    ohlcv.setLow(low);
                    ohlcv.setClose(close);
                    ohlcv.setVolumeFrom(volumeFrom);
                    ohlcv.setVolumeTo(volumeTo);

                    ohlcvList.add(ohlcv);
                });
        result = ohlcvList;

        if (isUpdate) {
            Date lastDate = queryHandler.getLastDateFromDailyChanges();
            DateTime fromDate = new DateTime(lastDate).plusDays(1);
            result = ohlcvList.stream()
                    .filter(x -> x.getDate().after(fromDate.toDate()))
                    .collect(Collectors.toList());
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
        optionalParams.forEach((key, value) ->
                        stringBuilder.append("&")
                                .append(key)
                                .append("=")
                                .append(value)
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
