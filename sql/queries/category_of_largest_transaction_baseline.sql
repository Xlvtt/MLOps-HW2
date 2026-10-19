SELECT
    state,
    argMax(category, amount) AS category,
    max(amount) AS max_transaction_amount
FROM hw2.transactions
GROUP BY state
ORDER BY state;

