-- =========================================================
-- Dimension Tables for NYC Yellow Taxi Star Schema
-- =========================================================

-- dim_date: calendar lookup
CREATE TABLE dim_date (
    date_key    BIGSERIAL PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    year        SMALLINT NOT NULL,
    quarter     SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    month_name  VARCHAR(10) NOT NULL,
    day         SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_name    VARCHAR(10) NOT NULL,
    is_weekend  BOOLEAN NOT NULL
);

-- dim_zones: 265 NYC taxi zones (loaded from CSV by loader)
CREATE TABLE dim_zones (
    location_id  INT PRIMARY KEY,
    borough      VARCHAR(50) NOT NULL,
    zone         VARCHAR(100) NOT NULL,
    service_zone VARCHAR(50) NOT NULL
);

-- dim_payment_type: payment method lookup
CREATE TABLE dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    description     VARCHAR(50) NOT NULL
);

-- dim_rate_code: rate code lookup
CREATE TABLE dim_rate_code (
    rate_code_id INT PRIMARY KEY,
    description  VARCHAR(50) NOT NULL
);

-- dim_vendor: taxi vendor lookup
CREATE TABLE dim_vendor (
    vendor_id   INT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL
);
