-- Run manually AFTER the synthetic notebooks; requires object ownership / APPLY TAG.
-- The tags contain no secrets, identifiers or private data.
ALTER TABLE uber_portfolio.bronze_ride_events
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'bronze');
ALTER TABLE uber_portfolio.silver_ride_quarantine
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'silver');
ALTER TABLE uber_portfolio.silver_current_rides
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'silver');
ALTER TABLE uber_portfolio.gold_ride_status
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'gold');
ALTER TABLE uber_portfolio.gold_zone_kpis
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'gold');
