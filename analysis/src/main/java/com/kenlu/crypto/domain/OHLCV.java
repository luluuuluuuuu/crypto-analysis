package com.kenlu.crypto.domain;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Data
public class OHLCV {

    private Product product;
    private Date date;
    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private BigDecimal volume;

    public OHLCV() {
    }
}
