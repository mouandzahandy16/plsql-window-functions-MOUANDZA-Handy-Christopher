CREATE TABLE customers (
  customer_id INT PRIMARY KEY,
  name VARCHAR(100),
  region VARCHAR(50),
  join_date DATE
);

CREATE TABLE products (
  product_id INT PRIMARY KEY,
  name VARCHAR(100),
  category VARCHAR(50),
  price DECIMAL(12,2)
);


CREATE TABLE transactions (
  transaction_id INT PRIMARY KEY,
  customer_id INT,
  product_id INT,
  sale_date DATE,
  quantity INT,
  amount DECIMAL(12,2),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id)
);


INSERT INTO customers VALUES (1001, 'Alice Kim', 'Kigali', '2024-01-10');
INSERT INTO customers VALUES (1002, 'Ben Mutesi', 'Gisenyi', '2023-11-15');
INSERT INTO customers VALUES (1003, 'Chantal Uwimana', 'Kigali', '2024-03-02');
INSERT INTO customers VALUES (1004, 'David Niyonsaba', 'Butare', '2022-07-20');
INSERT INTO customers VALUES (1005, 'Emma Habimana', 'Gisenyi', '2024-05-05');

INSERT INTO products VALUES (2001, 'Coffee Beans - Arabica', 'Beverages', 12000.00);
INSERT INTO products VALUES (2002, 'Espresso Shot', 'Beverages', 7000.00);
INSERT INTO products VALUES (2003, 'Cappuccino', 'Beverages', 9000.00);
INSERT INTO products VALUES (2004, 'Coffee Mug', 'Merch', 3000.00);
INSERT INTO products VALUES (2005, 'Premium Blend 500g', 'Beverages', 25000.00);

INSERT INTO transactions VALUES (3001, 1001, 2001, '2025-01-15', 2, 24000.00);
INSERT INTO transactions VALUES (3002, 1002, 2003, '2025-01-18', 1, 9000.00);
INSERT INTO transactions VALUES (3003, 1003, 2002, '2025-02-05', 3, 21000.00);
INSERT INTO transactions VALUES (3004, 1004, 2001, '2025-02-10', 1, 12000.00);
INSERT INTO transactions VALUES (3005, 1001, 2005, '2025-03-01', 1, 25000.00);
INSERT INTO transactions VALUES (3006, 1005, 2003, '2025-03-05', 2, 18000.00);
INSERT INTO transactions VALUES (3007, 1002, 2004, '2025-03-20', 1, 3000.00);
INSERT INTO transactions VALUES (3008, 1003, 2001, '2025-04-12', 1, 12000.00);
INSERT INTO transactions VALUES (3009, 1004, 2002, '2025-04-30', 2, 14000.00);
INSERT INTO transactions VALUES (3010, 1005, 2005, '2025-05-03', 1, 25000.00);


SELECT region,
       CONCAT(YEAR(sale_date), '-Q', QUARTER(sale_date)) AS year_quarter,
       product_id,
       product_name,
       total_revenue,
       revenue_rank
FROM (
  SELECT c.region,
         p.product_id,
         p.name AS product_name,
         YEAR(t.sale_date) AS sale_year,
         QUARTER(t.sale_date) AS sale_quarter,
         SUM(t.amount) AS total_revenue,
         RANK() OVER (
           PARTITION BY c.region, YEAR(t.sale_date), QUARTER(t.sale_date)
           ORDER BY SUM(t.amount) DESC
         ) AS revenue_rank,
         t.sale_date
  FROM transactions t
  JOIN customers c ON t.customer_id = c.customer_id
  JOIN products p ON t.product_id = p.product_id
  GROUP BY c.region, p.product_id, p.name, YEAR(t.sale_date), QUARTER(t.sale_date), t.sale_date
) ranked
WHERE revenue_rank <= 5
ORDER BY region, year_quarter, revenue_rank;


WITH monthly AS (
  SELECT c.region,
         DATE_FORMAT(s.sale_date, '%Y-%m-01') AS month_start,
         SUM(s.amount) AS month_total
  FROM transactions s
  JOIN customers c ON s.customer_id = c.customer_id
  GROUP BY c.region, DATE_FORMAT(s.sale_date, '%Y-%m-01')
)
SELECT region,
       month_start,
       month_total,
       SUM(month_total) OVER (PARTITION BY region ORDER BY month_start
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM monthly
ORDER BY region, month_start;


WITH monthly AS (
  SELECT c.region,
         DATE_FORMAT(s.sale_date, '%Y-%m-01') AS month_start,
         SUM(s.amount) AS month_total
  FROM transactions s
  JOIN customers c ON s.customer_id = c.customer_id
  GROUP BY c.region, DATE_FORMAT(s.sale_date, '%Y-%m-01')
)
SELECT region,
       month_start,
       month_total,
       LAG(month_total) OVER (PARTITION BY region ORDER BY month_start) AS prev_month_total,
       ROUND(
         CASE 
           WHEN LAG(month_total) OVER (PARTITION BY region ORDER BY month_start) IS NULL 
             THEN NULL
           WHEN LAG(month_total) OVER (PARTITION BY region ORDER BY month_start) = 0 
             THEN NULL
           ELSE (month_total - LAG(month_total) OVER (PARTITION BY region ORDER BY month_start))
                / LAG(month_total) OVER (PARTITION BY region ORDER BY month_start) * 100
         END, 2) AS mom_pct_change
FROM monthly
ORDER BY region, month_start;


WITH cust_spend AS (
   SELECT c.customer_id,
          c.name,
          c.region,
          COALESCE(SUM(t.amount), 0) AS total_spent
   FROM customers c
   LEFT JOIN transactions t ON c.customer_id = t.customer_id
   GROUP BY c.customer_id, c.name, c.region
)
SELECT customer_id,
       name,
       region,
       total_spent,
       NTILE(4) OVER (PARTITION BY region ORDER BY total_spent DESC) AS spend_quartile,
       CUME_DIST() OVER (PARTITION BY region ORDER BY total_spent DESC) AS cume_distribution
FROM cust_spend
ORDER BY region, total_spent DESC;


WITH monthly AS (
  SELECT c.region,
         DATE_FORMAT(s.sale_date, '%Y-%m-01') AS month_start,
         SUM(s.amount) AS month_total
  FROM transactions s
  JOIN customers c ON s.customer_id = c.customer_id
  GROUP BY c.region, DATE_FORMAT(s.sale_date, '%Y-%m-01')
)
SELECT region,
       month_start,
       month_total,
       ROUND(
         AVG(month_total) OVER (PARTITION BY region ORDER BY month_start
                                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2
       ) AS moving_avg_3m
FROM monthly
ORDER BY region, month_start;

SELECT * FROM transactions;
SELECT * FROM Products;
SELECT * FROM Customers;