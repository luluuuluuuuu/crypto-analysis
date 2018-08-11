package com.kenlu.crypto.extraction.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.kenlu.crypto.domain.Crypto;
import com.kenlu.crypto.domain.OHLCV;
import com.kenlu.crypto.domain.Stock;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Component
public class DataExtractor {

    private QueryHandler queryHandler;

    public DataExtractor(QueryHandler queryHandler) {
        this.queryHandler = queryHandler;
    }

    public List<OHLCV> getCryptoDailyOHLCVs(Crypto crypto, int numOfDays, long toTimestamp, boolean isUpdate) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("extraParams", "crypto-analysis");
        params.put("limit", Integer.toString(numOfDays - 1));
        params.put("toTs", Long.toString(toTimestamp));

        List<OHLCV> ohlcvList = new ArrayList<>();
        List<OHLCV> result;

        this.getCryptoHistoDay(crypto.name(), "USD", params)
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

                    ohlcv.setProduct(crypto);
                    ohlcv.setDate(date);
                    ohlcv.setOpen(open);
                    ohlcv.setHigh(high);
                    ohlcv.setLow(low);
                    ohlcv.setClose(close);
                    ohlcv.setVolume(volumeTo.subtract(volumeFrom));

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

    public List<OHLCV> getStockDailyOHLCVs(Stock stock, int numOfDays, long toTimestamp, boolean isUpdate) throws Exception {
        List<OHLCV> ohlcvList = new ArrayList<>();
        List<OHLCV> result;

        this.getStockHistoDay(stock.name())
                .iterator()
                .forEachRemaining(x -> {
                    JsonObject jsonObject = x.getAsJsonObject();
                    Date date = new Date();
                    try {
                        date = new SimpleDateFormat("yyyy-MM-dd")
                                .parse(jsonObject.get("date").getAsString());
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    Date toDate = new Date(toTimestamp * 1000);
                    Date fromDate = new DateTime(toDate).minusDays(numOfDays).toDate();

                    if (date.after(fromDate)) {
                        BigDecimal open = jsonObject.get("open").getAsBigDecimal();
                        BigDecimal high = jsonObject.get("high").getAsBigDecimal();
                        BigDecimal low = jsonObject.get("low").getAsBigDecimal();
                        BigDecimal close = jsonObject.get("close").getAsBigDecimal();
                        BigDecimal volume = jsonObject.get("volume").getAsBigDecimal();
                        OHLCV ohlcv = new OHLCV();

                        ohlcv.setProduct(stock);
                        ohlcv.setDate(date);
                        ohlcv.setOpen(open);
                        ohlcv.setHigh(high);
                        ohlcv.setLow(low);
                        ohlcv.setClose(close);
                        ohlcv.setVolume(volume);

                        ohlcvList.add(ohlcv);
                    }
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

    private JsonObject getCryptoHistoDay(String fsym, String tsym, Map<String, Object> optionalParams) throws Exception {
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

        JsonObject jsonObject = (JsonObject) this.getHttpResponse(requestUrl);

        return jsonObject;
    }

    private JsonArray getStockHistoDay(String fsym) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("https://api.iextrading.com/1.0/stock/")
                .append(fsym)
                .append("/chart/5y");
        String requestUrl = stringBuilder.toString();

        JsonArray jsonArray = (JsonArray) this.getHttpResponse(requestUrl);

        return jsonArray;
    }

    private JsonElement getHttpResponse(String requestUrl) throws InterruptedException, java.util.concurrent.ExecutionException, IOException {
        CloseableHttpAsyncClient client = HttpAsyncClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .build();
        client.start();
        HttpGet request = new HttpGet(requestUrl);
        Future<HttpResponse> future = client.execute(request, null);
        HttpResponse response = future.get();
        client.close();

        JsonParser jsonParser = new JsonParser();
        JsonElement jsonElement = jsonParser
                .parse(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));

        return jsonElement;
    }

}
