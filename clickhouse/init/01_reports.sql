CREATE DATABASE IF NOT EXISTS reports;

CREATE TABLE IF NOT EXISTS reports.reports_mart
(
    user_id            String,
    report_date        Date,
    prosthesis_id      String,
    prosthesis_model   String,
    client_name        String,
    country            String,
    movements_count    UInt32,
    avg_reaction_ms    Float32,
    max_reaction_ms    UInt32,
    recognition_errors UInt32,
    avg_battery_level  Float32,
    loaded_at          DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(report_date)
ORDER BY (user_id, report_date, prosthesis_id);

CREATE TABLE IF NOT EXISTS reports.etl_loaded_periods
(
    loaded_until Date,
    loaded_at    DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY loaded_at;
