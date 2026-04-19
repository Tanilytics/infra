ALTER TABLE analytics.events
ADD COLUMN IF NOT EXISTS event_name String AFTER event_type;
