-- =========================================================
-- Seed Dimension Tables
-- dim_zones is loaded by the Python loader from CSV
-- =========================================================

-- dim_date: Jan 1 to Feb 29, 2024
INSERT INTO dim_date (full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
SELECT
    d::DATE,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Month')),
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(ISODOW FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Day')),
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2024-01-01'::DATE, '2024-02-29'::DATE, '1 day') AS d;

-- dim_payment_type
INSERT INTO dim_payment_type (payment_type_id, description) VALUES
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip');

-- dim_rate_code
INSERT INTO dim_rate_code (rate_code_id, description) VALUES
(1, 'Standard rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau/Westchester'),
(5, 'Negotiated fare'),
(6, 'Group ride'),
(99, 'Unknown');

-- dim_vendor
INSERT INTO dim_vendor (vendor_id, name) VALUES
(1, 'Creative Mobile Technologies'),
(2, 'VeriFone Inc.');
