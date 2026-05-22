-- Minimal alarms table for Testcontainers — keeps the test stack independent
-- of the full opennms-model-jakarta Liquibase changelog.
CREATE TABLE IF NOT EXISTS service (
  serviceid SERIAL PRIMARY KEY,
  servicename VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS alarms (
  alarmid SERIAL PRIMARY KEY,
  eventuei VARCHAR(256) NOT NULL,
  nodeid INTEGER,
  ipaddr VARCHAR(40),
  serviceid INTEGER REFERENCES service(serviceid),
  reductionkey VARCHAR(256) NOT NULL UNIQUE,
  alarmtype INTEGER,
  counter INTEGER DEFAULT 0,
  severity INTEGER NOT NULL DEFAULT 0,
  firsteventtime TIMESTAMP,
  lasteventtime TIMESTAMP,
  alarmacktime TIMESTAMP,
  alarmackuser VARCHAR(64),
  description TEXT,
  logmsg TEXT,
  ifindex INTEGER,
  tticketstate INTEGER,
  location VARCHAR(64),
  -- Denormalized event_* columns (replace old lasteventid FK; horizon 36.0.0)
  event_tsid BIGINT,
  event_uei VARCHAR(256),
  event_source VARCHAR(256),
  event_severity INTEGER,
  event_timestamp TIMESTAMP,
  event_node_id BIGINT,
  event_log_msg TEXT,
  last_event_data TEXT
);

INSERT INTO service (servicename) VALUES ('ICMP'), ('HTTP') ON CONFLICT DO NOTHING;
