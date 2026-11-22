ALTER TABLE clean_yellow_trips ADD COLUMN IF NOT EXISTS source_month TEXT;
UPDATE clean_yellow_trips SET source_month = '2026-08' WHERE source_month IS NULL;
CREATE INDEX IF NOT EXISTS clean_yellow_trips_source_month_idx ON clean_yellow_trips (source_month);
