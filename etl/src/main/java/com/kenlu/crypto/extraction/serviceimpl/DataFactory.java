package com.kenlu.crypto.extraction.serviceimpl;

import com.crypto.cryptocompare.api.CryptoCompareApi;
import com.google.gson.JsonObject;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@Component
public class DataFactory extends CryptoCompareApi {

    protected Map<String, Double> getDailyChanges(String crypto, int numOfDays, long toTimestamp) {
        Map<String, Object> params = new HashMap<>();
        params.put("extraParams", "crypto-analysis");
        params.put("limit", Integer.toString(numOfDays - 1));
        params.put("toTs", Long.toString(toTimestamp));

        Map<String, Double> row = new TreeMap<>();

        histoDay(crypto, "USD", params)
                .get("Data")
                .getAsJsonArray()
                .iterator()
                .forEachRemaining(x -> {
                    JsonObject jsonObject = x.getAsJsonObject();
                    Date date = new Date(jsonObject.get("time").getAsLong() * 1000);
                    DateFormat f = new SimpleDateFormat("yyyy/MM/dd");
                    double open = jsonObject.get("open").getAsDouble();
                    double close = jsonObject.get("close").getAsDouble();
                    double changes = close / open;

                    row.put(f.format(date), changes);
                });

        return row;
    }

}
