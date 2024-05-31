-- Use PostgreSQL
-- What is the total amount each customer spent at the restaurant?
SELECT customer_id, SUM(price) as total_spent
FROM sales T1
LEFT JOIN menu T2
    ON T1.product_id = T2.product_id
GROUP BY customer_id
ORDER BY total_spent DESC;

-- How many days has each customer visited the restaurant?
SELECT customer_id, COUNT(DISTINCT order_date) as total_days
FROM sales
GROUP BY customer_id;

-- What was the first item from the menu purchased by each customer?
SELECT DISTINCT
    T1.customer_id,
    string_agg(DISTINCT T1.product_name, ', ') as product_name
FROM (
    SELECT *,
       dense_rank() OVER(PARTITION BY T1.customer_id ORDER BY order_date) as rank
    FROM sales T1
    LEFT JOIN menu T2
        ON T1.product_id = T2.product_id
) T1
WHERE
    T1.rank = 1
GROUP BY T1.customer_id;

-- What is the most purchased item on the menu and how many times was it purchased by all customers?
SELECT product_name, COUNT(*) as total_orders
FROM sales T1
LEFT JOIN menu T2
    ON T1.product_id = T2.product_id
GROUP BY product_name
ORDER BY 2 DESC
LIMIT 1;

-- Which item was the most popular for each customer?
SELECT customer_id, product_name, total_orders
FROM (
    SELECT customer_id, product_name, COUNT(*) as total_orders,
       dense_rank() OVER(PARTITION BY customer_id ORDER BY COUNT(*) DESC) as rank_num
    FROM sales T1
    LEFT JOIN menu T2
        ON T1.product_id = T2.product_id
    GROUP BY customer_id, product_name
) T1
WHERE T1.rank_num = 1;

-- Which item was purchased first by the customer after they became a member?
SELECT customer_id, product_name, order_date
FROM (
    SELECT T1.customer_id,
        dense_rank() over (PARTITION BY T1.customer_id ORDER BY order_date) as rank_num,
        product_name,
        order_date
    FROM sales T1
    LEFT JOIN members T2
        ON T1.customer_id = T2.customer_id
    LEFT JOIN menu T3
        ON T1.product_id = T3.product_id
    WHERE
        T1.order_date >= T2.join_date
) T1
WHERE
    T1.rank_num = 1;

-- Which item was purchased just before the customer became a member?
SELECT customer_id,
       string_agg(DISTINCT product_name, ', ') as product_name,
       order_date
FROM (
    SELECT T1.customer_id,
        dense_rank() over (PARTITION BY T1.customer_id ORDER BY order_date) as rank_num,
        product_name,
        order_date
    FROM sales T1
    LEFT JOIN members T2
        ON T1.customer_id = T2.customer_id
    LEFT JOIN menu T3
        ON T1.product_id = T3.product_id
    WHERE
        T1.order_date < T2.join_date
) T1
WHERE
    T1.rank_num = 1
GROUP BY customer_id, order_date;

-- What is the total items and amount spent for each member before they became a member?
SELECT T1.customer_id,
       COUNT(*) as total_items,
       SUM(price) as amount_spent
FROM sales T1
LEFT JOIN members T2
    ON T1.customer_id = T2.customer_id
LEFT JOIN menu T3
    ON T1.product_id = T3.product_id
WHERE
    T1.order_date < T2.join_date
GROUP BY T1.customer_id
ORDER BY T1.customer_id;

-- If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?
SELECT customer_id,
       SUM(
               CASE
                   WHEN product_name = 'sushi' THEN price * 20
                   ELSE price * 10
                   END) as customer_points
FROM sales T1
         LEFT JOIN menu T2
                   ON T1.product_id = T2.product_id
GROUP BY customer_id
ORDER BY customer_id;

-- In the first week after a customer joins the program (including their join date) they earn 2x points on all items,
-- not just sushi - how many points do customer A and B have at the end of January?
SELECT *
FROM sales T1
LEFT JOIN menu T2
    ON T1.product_id = T2.product_id
LEFT JOIN members T3
    ON T1.customer_id = T3.customer_id
WHERE
    T1.order_date >= T3.join_date AND
    date_part('month', order_date) = 1;

WITH program_last_date AS (
    SELECT join_date,
       (join_date + INTERVAL '6 days')::date as program_last_date,
       customer_id
    FROM members
)
SELECT T1.customer_id,
       SUM(CASE
            WHEN order_date BETWEEN join_date AND T3.program_last_date THEN price * 20
            WHEN (order_date NOT BETWEEN join_date AND T3.program_last_date) AND (product_name = 'sushi') THEN price * 20
            ELSE price * 10
       END) as customer_point
FROM sales T1
LEFT JOIN menu T2
    ON T1.product_id = T2.product_id
LEFT JOIN program_last_date T3
    ON T1.customer_id = T3.customer_id
WHERE
    order_date <= '2021-01-31' AND order_date >= join_date
GROUP BY T1.customer_id
ORDER BY customer_id;

-- The following questions are related creating basic data tables that Danny and his team
-- can use to quickly derive insights without needing to join the underlying tables using SQL.
SELECT T1.customer_id,
       T1.order_date,
       T3.product_name,
       T3.price,
        CASE
            WHEN T1.order_date >= T2.join_date THEN 'Y'
            ELSE 'N'
        END as member
FROM sales T1
LEFT JOIN members T2
    ON T1.customer_id = T2.customer_id
LEFT JOIN menu T3
    ON T1.product_id = T3.product_id
ORDER BY customer_id, order_date;

-- Danny also requires further information about the ranking of customer products,
-- but he purposely does not need the ranking for non-member purchases so
-- he expects null ranking values for the records when customers are not yet part of the loyalty program.
SELECT T1.*,
    CASE
        WHEN T1.member = 'N' THEN null
        ELSE dense_rank() OVER(PARTITION BY customer_id, member ORDER BY order_date)
    END as ranking
FROM (
    SELECT T1.customer_id,
           T1.order_date,
           T3.product_name,
           T3.price,
            CASE
                WHEN T1.order_date >= T2.join_date THEN 'Y'
                ELSE 'N'
            END as member
    FROM sales T1
    LEFT JOIN members T2
        ON T1.customer_id = T2.customer_id
    LEFT JOIN menu T3
        ON T1.product_id = T3.product_id
    ORDER BY customer_id, order_date
) T1