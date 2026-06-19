WITH reconciliation_summary AS (
    SELECT
        COUNT(*) AS total_records,
        SUM(
            CASE
                WHEN reconciliation_status IN (
                    'missing_purchase_event',
                    'delayed_purchase_event',
                    'payment_not_settled',
                    'paid_cancelled_order',
                    'gateway_timeout_near_order',
                    'purchase_without_order'
                ) THEN 1
                ELSE 0
            END
        ) AS exception_records
    FROM {{ ref('gold_order_reconciliation') }}
),

violations AS (
    SELECT *
    FROM reconciliation_summary
    WHERE total_records > 0
      AND exception_records::DOUBLE PRECISION / total_records > 0.25
)

SELECT *
FROM violations
