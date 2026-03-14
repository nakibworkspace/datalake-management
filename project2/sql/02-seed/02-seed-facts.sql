-- =========================================================
-- Fact data is loaded by the Python loader (scripts/load_data.py)
-- This file runs ANALYZE after the loader finishes
-- =========================================================

-- Placeholder: the loader handles bulk data loading via COPY
-- This file is kept so init-runner.sh doesn't break

ANALYZE dim_date;
ANALYZE dim_zones;
ANALYZE dim_payment_type;
ANALYZE dim_rate_code;
ANALYZE dim_vendor;
