CREATE TABLE alpaca.spot_1h
(
    id                                      BIGINT,
    symbol                                  VARCHAR(20) NOT NULL,
    open_time                               TIMESTAMP NOT NULL,
    open_price                              DECIMAL(24,8),
    high_price                              DECIMAL(24,8),
    low_price                               DECIMAL(24,8),
    close_price                             DECIMAL(24,8),
    volume_stock                            DECIMAL(24,8),
    volume_dollar                           DECIMAL(24,8),
    vwap                                    DECIMAL(24,8),
    trades                                  INTEGER,

    PRIMARY KEY (id),
    UNIQUE (symbol, open_time)
);