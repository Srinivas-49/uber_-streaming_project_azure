-- SECURITY REVIEW TEMPLATE; do not run without replacing identities/catalogs and checking current grants.
-- The following statements illustrate design decisions, NOT proven deployed policies.
-- Sensitive passenger/driver fields should be masked in downstream consumption.
CREATE SCHEMA IF NOT EXISTS uber_analytics.secure;
CREATE OR REPLACE FUNCTION uber_analytics.secure.mask_phone(phone STRING)
RETURN CASE
  WHEN is_account_group_member('uber-pii-authorized') THEN phone
  ELSE regexp_replace(phone, '[0-9]', 'X')
END;

-- Apply to an approved, existing Gold table only after verifying exact naming:
-- ALTER TABLE uber_analytics.gold.dim_passenger ALTER COLUMN passenger_phone
-- SET MASK uber_analytics.secure.mask_phone;
-- GRANT USE CATALOG ON CATALOG uber_analytics TO `uber-analysts`;
-- GRANT USE SCHEMA ON SCHEMA uber_analytics.gold TO `uber-analysts`;
-- GRANT SELECT ON TABLE uber_analytics.gold.fact TO `uber-analysts`;
-- Check inherited privileges, compute permissions and group membership before approval.
-- Inspect Unity Catalog lineage in the workspace, not through assumed screenshots.
