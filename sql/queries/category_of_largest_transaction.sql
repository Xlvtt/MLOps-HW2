SELECT
    state,
    argMaxMerge(category_state) AS category,
    maxMerge(amount_state) AS max_transaction_amount
FROM hw2.state_category_max
GROUP BY state
ORDER BY state;

