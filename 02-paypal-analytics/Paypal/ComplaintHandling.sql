USE PaypalAnalytic;
GO

/*
• dispute nào xảy ra
• ngày xảy ra
• lý do dispute
• trạng thái dispute
• seller liên quan
• số tiền tranh chấp
• seller có protection hay không
*/
SELECT      dd.dispute_id,
            d.full_date                AS dispute_date,
            dd.reason                  AS dispute_reason,
            dd.status                  AS dispute_status,
            dd.dispute_state,
            dd.dispute_life_cycle_stage,
            dd.dispute_channel,
            s.seller_name,
            s.seller_email,
            s.seller_merchant_id,
            f.dispute_amount,
            f.transaction_count,
            f.money_movement_amount,
            f.seller_protection_eligible
FROM        gold.fact_disputePP02_dispute f
LEFT JOIN   gold.dim_disputePP02_dispute dd ON f.dispute_key = dd.dispute_key
LEFT JOIN   gold.dim_disputePP02_Seller s ON f.seller_key = s.seller_key
LEFT JOIN   gold.dim_date d ON f.date_key = d.date_key
ORDER BY    d.full_date DESC;
GO
