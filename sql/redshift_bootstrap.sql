-- Run in Amazon Redshift Serverless query editor v2 or any SQL client
-- connected to the target database, for example dev.
-- Replace the IAM role ARN if you did not attach a default role to the namespace.

CREATE EXTERNAL SCHEMA IF NOT EXISTS bronze
FROM DATA CATALOG
DATABASE 'bronze'
IAM_ROLE default
REGION 'us-east-1';

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

GRANT USAGE ON SCHEMA bronze TO PUBLIC;
GRANT USAGE, CREATE ON SCHEMA silver TO PUBLIC;
GRANT USAGE, CREATE ON SCHEMA gold TO PUBLIC;

-- Sanity checks. These should return the Iceberg tables registered by Spark/Glue:
-- clickstream, server_logs, order_changes, shipment_data, users, products.
SELECT schemaname, tablename
FROM svv_external_tables
WHERE schemaname = 'bronze'
ORDER BY tablename;
