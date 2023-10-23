CREATE VIEW mean_client_daily_transactions AS
WITH clients AS (
    SELECT 
        c.customer_id, a.account_id, c.name 
    FROM
        customer c 
    JOIN 
        account a 
    ON
        c.customer_id = a.customer_id
),
transactions AS (
    SELECT
        c.customer_id, c.account_id, c.name,
        b.amount AS amount, 'bankslip' AS transaction_type, b.dt AS date
    FROM
        clients c
        LEFT JOIN bankslip b 
        ON c.account_id = b.account_id

    UNION ALL

    SELECT
        c.customer_id, c.account_id, c.name,
        ps.amount AS amount, 'pix_send' AS transaction_type, ps.dt AS date
    FROM
        clients c
        LEFT JOIN pix_send ps 
        ON c.account_id = ps.account_id

    UNION ALL

    SELECT
        c.customer_id, c.account_id, c.name,
        pr.amount AS amount, 'pix_received' AS transaction_type, pr.dt AS date
    FROM
        clients c
        LEFT JOIN pix_received pr
        ON c.account_id = pr.account_id

    UNION ALL

    SELECT
        c.customer_id, c.account_id, c.name,
        p2p.amount AS amount, 'p2p_tef_send' AS transaction_type, p2p.dt AS date
    FROM
        clients c
        LEFT JOIN p2p_tef p2p
        ON c.account_id = p2p.account_id_destination

    UNION ALL

    SELECT
        c.customer_id, c.account_id, c.name,
        p2p.amount AS amount, 'p2p_tef_received' AS transaction_type, p2p.dt AS date
    FROM
        clients c
        LEFT JOIN p2p_tef p2p
        ON c.account_id = p2p.account_id_source
)
SELECT
    customer_id, account_id, name, date, transaction_type, AVG(amount) AS mean_value 
FROM 
    transactions
GROUP BY
    customer_id,
    account_id,
    name,
    date,
    transaction_type;
