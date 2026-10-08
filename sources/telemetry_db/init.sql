CREATE TABLE prosthesis_telemetry (
    id            BIGSERIAL   PRIMARY KEY,
    prosthesis_id VARCHAR(32) NOT NULL,
    event_time    TIMESTAMP   NOT NULL,
    event_type    VARCHAR(32) NOT NULL,
    reaction_ms   INT,
    battery_level SMALLINT    NOT NULL
);

CREATE INDEX idx_telemetry_event_time ON prosthesis_telemetry (event_time);

SELECT setseed(0.42);

INSERT INTO prosthesis_telemetry (prosthesis_id, event_time, event_type, reaction_ms, battery_level)
SELECT
    prosthesis_id,
    day + random() * INTERVAL '1 day',
    CASE WHEN r < 0.05 THEN 'recognition_error' ELSE 'movement' END,
    CASE WHEN r < 0.05 THEN NULL ELSE 40 + floor(random() * 90)::int END,
    20 + floor(random() * 80)::int
FROM (
    SELECT p.prosthesis_id, d.day, n, random() AS r
    FROM unnest(ARRAY['PR-1001', 'PR-1002', 'PR-2001', 'PR-3001']) AS p (prosthesis_id)
    CROSS JOIN generate_series(CURRENT_DATE - 30, CURRENT_DATE, INTERVAL '1 day') AS d (day)
    CROSS JOIN generate_series(1, 100) AS n
) AS events
WHERE n <= 60 + (hashtext(prosthesis_id || day::text) & 2147483647) % 41;
