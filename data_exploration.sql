-- *************
-- DATA CLEANING
-- *************

-- 1. Create events_cleaned with Parsed Columns
CREATE TABLE events_cleaned AS
SELECT 
  *,  -- keep all original columns
  -- Parse amount
  (REPLACE(value, '''', '"')::json->>'amount')::double precision AS transaction_amount,
  -- Parse offer_id with fallback for both key styles
  COALESCE(
    REPLACE(value, '''', '"')::json->>'offer_id',
    REPLACE(value, '''', '"')::json->>'offer id'
  ) AS offer_id,
  -- Parse reward
  (REPLACE(value, '''', '"')::json->>'reward')::double precision AS reward_amount
FROM events;




-- *************************
-- EXPLORATORY DATA ANALYSIS
-- *************************

-- 1. Identify events for funnel analysis
SELECT 
	DISTINCT(event)
FROM events_cleaned

/* Identified events used in the funnel: offer received, offer viewed, transactions, offer completed.
Will be removing 'transactions' from the funnel analysis as this is synonymous as 'offer completed' for the reward flow. */


-- 2. Basic funnel count
SELECT event, COUNT(DISTINCT customer_id) AS event_count
FROM events_cleaned
WHERE event IN ('offer received', 'offer viewed', 'offer completed')
GROUP BY event
ORDER BY 2 DESC


-- 3. Calculate dropoff rate
WITH funnel_data AS (
    SELECT event, COUNT(DISTINCT customer_id) AS event_count
	FROM events_cleaned
	WHERE event IN ('offer received','offer viewed','offer completed')
	GROUP BY 1
	ORDER BY 2 DESC
)
SELECT 
    event, 
    event_count,
    LAG(event_count, 1) OVER (ORDER BY event_count DESC) AS previous_count,
    (LAG(event_count, 1) OVER (ORDER BY event_count DESC) - event_count) * 100.0 / LAG(event_count, 1) OVER (ORDER BY event_count DESC) AS churn_rate
FROM funnel_data
ORDER BY 2 DESC;

/* The biggest dropoff happens between offer viewed and offer completed (24%). Investigate why and how to optimize this part. */


-- 4. Dropoff rate by offer type
WITH funnel_data AS (
    SELECT 
    o.offer_type,
    e.event,
    COUNT(DISTINCT e.customer_id) AS event_count
  FROM events_cleaned e
  JOIN offers o ON e.offer_id = o.offer_id
  WHERE e.event IN ('offer received', 'offer viewed', 'offer completed')
  GROUP BY 1,2
)
SELECT 
    offer_type, 
    event,
    event_count,
    LAG(event_count) OVER (PARTITION BY offer_type ORDER BY event_count DESC) AS previous_count,
    ROUND((LAG(event_count) OVER (PARTITION BY offer_type ORDER BY event_count DESC) - event_count) * 100.0 / 
		LAG(event_count, 1) OVER (PARTITION BY offer_type ORDER BY event_count DESC), 2) AS churn_rate
FROM funnel_data
ORDER BY 1,3 DESC

/* For lag function, need to use PARTITION BY to dissect by offer type. */


-- 5. Dropoff rate by offer difficulty
WITH funnel_data AS (
    SELECT 
    difficulty,
    e.event,
    COUNT(DISTINCT e.customer_id) AS event_count
  FROM events_cleaned e
  JOIN offers o ON e.offer_id = o.offer_id
  WHERE e.event IN ('offer received', 'offer viewed', 'offer completed')
  GROUP BY 1,2
)
SELECT 
    difficulty, 
    event,
    event_count,
    LAG(event_count) OVER (PARTITION BY difficulty ORDER BY event_count DESC) AS previous_count,
    ROUND((LAG(event_count) OVER (PARTITION BY difficulty ORDER BY event_count DESC) - event_count) * 100.0 / 
		LAG(event_count, 1) OVER (PARTITION BY difficulty ORDER BY event_count DESC), 2) AS churn_rate
FROM funnel_data
WHERE difficulty >=5
ORDER BY 1, 
	CASE event
		WHEN 'offer received' THEN 1
		WHEN 'offer viewed' THEN 2
		WHEN 'offer completed' THEN 3
	END

/* Confirmed that difficulty is correlated with higher churn rate. Highest difficulty (20) has a churn rate of 53%, while lowest difficulty (5) has a churn rate of 23%. */


-- 6. Dropoff rate by offer reward
WITH funnel_data AS (
    SELECT 
    reward,
    e.event,
    COUNT(DISTINCT e.customer_id) AS event_count
  FROM events_cleaned e
  JOIN offers o ON e.offer_id = o.offer_id
  WHERE e.event IN ('offer received', 'offer viewed', 'offer completed')
  GROUP BY 1,2
)
SELECT 
    reward, 
    event,
    event_count,
    LAG(event_count) OVER (PARTITION BY reward ORDER BY event_count DESC) AS previous_count,
    ROUND((LAG(event_count) OVER (PARTITION BY reward ORDER BY event_count DESC) - event_count) * 100.0 / 
		LAG(event_count, 1) OVER (PARTITION BY reward ORDER BY event_count DESC), 2) AS churn_rate
FROM funnel_data
WHERE reward >= 2
ORDER BY 1, 3 DESC

/* Highest churn rate is with reward = 10. Is there a bug? */


--7. Dropoff rate by duration
WITH funnel_data AS (
    SELECT 
    duration,
    e.event,
    COUNT(DISTINCT e.customer_id) AS event_count
  FROM events_cleaned e
  JOIN offers o ON e.offer_id = o.offer_id
  WHERE e.event IN ('offer received', 'offer viewed', 'offer completed')
  GROUP BY 1,2
)
SELECT 
    duration, 
    event,
    event_count,
    LAG(event_count) OVER (PARTITION BY duration ORDER BY event_count DESC) AS previous_count,
    ROUND((LAG(event_count) OVER (PARTITION BY duration ORDER BY event_count DESC) - event_count) * 100.0 / 
		LAG(event_count, 1) OVER (PARTITION BY duration ORDER BY event_count DESC), 2) AS churn_rate
FROM funnel_data
WHERE duration >= 5
ORDER BY 1, 3 DESC
