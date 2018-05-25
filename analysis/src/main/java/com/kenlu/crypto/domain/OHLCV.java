package com.kenlu.crypto.domain;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Data
public class OHLCV {

    private Crypto crypto;
    private Date date;
    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private BigDecimal volumeFrom;
    private BigDecimal volumeTo;

    public OHLCV() {
    }
}
