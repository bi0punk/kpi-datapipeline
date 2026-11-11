-- Esquema mínimo para la demo y para DB_TABLE=test.kpis
CREATE SCHEMA IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.kpis (
    kpi_key   TEXT   NOT NULL,
    kpi_value TEXT   NOT NULL,
    vcount    BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (kpi_key, kpi_value)
);

CREATE INDEX IF NOT EXISTS idx_kpis_key ON test.kpis (kpi_key);
