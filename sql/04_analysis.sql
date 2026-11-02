-- Query 1: Average AQI per city per year
SELECT 
    c.city,
    d.year,
        ROUND(AVG(f.aqi), 1) AS avg_aqi
FROM workspace.aqi.fact_aqi_daily f
JOIN workspace.aqi.dim_city c ON f.city_id = c.city_id
JOIN workspace.aqi.dim_date d ON f.date_id = d.date_id
WHERE f.aqi_more_500 = false
    AND c.city IN ('Delhi', 'Mumbai', 'Bengaluru', 'Chennai', 'Lucknow', 'Ahmedabad', 'Hyderabad')
GROUP BY c.city, d.year
ORDER BY c.city, d.year;

-- Query 2: Year=over-year AQI change per city (2015- 2019)
WITH yearly AS (
    SELECT
        c.city,
        d.year,
        ROUND(AVG(f.aqi), 1) AS avg_aqi
    FROM workspace.aqi.fact_aqi_daily f
    JOIN workspace.aqi.dim_city c ON f.city_id = c.city_id
    JOIN workspace.aqi.dim_date d ON f.date_id = d.date_id
    WHERE f.aqi_more_500 = false 
        AND d.year BETWEEN 2015 AND 2019
        AND c.city IN ('Delhi', 'Bengaluru', 'Chennai', 'Lucknow', 'Hyderabad')
    GROUP BY c.city, d.year
)
SELECT
    city,
    year,
    avg_aqi,
    LAG(avg_aqi) OVER (PARTITION BY city ORDER BY year) AS prev_year_aqi,
    ROUND(avg_aqi - LAG(avg_aqi) OVER (PARTITION BY city ORDER BY year), 1) AS change
FROM yearly
ORDER BY city, year;

-- Query 3: Worst month for each city
WITH monthly AS (
    SELECT
        c.city,
        d.month,
        d.month_name,
        ROUND(AVG(f.aqi), 1) AS avg_aqi
    FROM workspace.aqi.fact_aqi_daily f
    JOIN workspace.aqi.dim_city c ON f.city_id = c.city_id
    JOIN workspace.aqi.dim_date d ON f.date_id = d.date_id
    WHERE f.aqi_more_500 = false
        AND c.city IN ('Delhi', 'Bengaluru', 'Chennai', 'Lucknow', 'Hyderabad')
    GROUP BY c.city, d.month, d.month_name
),
ranked AS (
    SELECT
        city,
        month_name,
        avg_aqi,
        RANK() OVER (PARTITION BY city ORDER BY avg_aqi DESC) AS rnk
    FROM monthly
)
SELECT city, month_name AS worst_month, avg_aqi
FROM ranked
WHERE rnk = 1
ORDER BY avg_aqi DESC;

-- Query 4: Lockdown effect - same period, 2019 vs 2020
SELECT
    c.city,
    ROUND(AVG(CASE WHEN d.year = 2019 THEN f.pm25 END), 1) AS pm25_2019,
    ROUND(AVG(CASE WHEN d.year = 2020 THEN f.pm25 END), 1) AS pm25_2020,
    ROUND(
        (AVG(CASE WHEN d.year = 2020 THEN f.pm25 END) - AVG (CASE WHEN d.year = 2019 THEN f.pm25 END)) / AVG(CASE WHEN d.year = 2019 THEN f.pm25 END) * 100
    , 1) AS pct_change
FROM workspace.aqi.fact_aqi_daily f
JOIN workspace.aqi.dim_city c ON f.city_id = c.city_id
JOIN workspace.aqi.dim_date d ON f.date_id = d.date_id
WHERE d.year IN (2019, 2020)
    AND DATE_FORMAT(d.date, 'MM-dd') BETWEEN '03-25' AND '05-31'
GROUP BY c.city
HAVING pm25_2019 IS NOT NULL AND pm25_2020 IS NOT NULL
ORDER BY pct_change;

-- Query 5: City ranking by share of Poor-or-worse days in 2019
WITH city_2019 AS (
    SELECT
        c.city, 
        c.region,
        COUNT(*) AS days_with_data,
        SUM(CASE WHEN f.aqi_category IN ('Poor', 'Very Poor', 'Severe') THEN 1 ELSE 0 END) AS poor_days,
        ROUND(AVG(f.aqi), 1) AS avg_aqi
    FROM workspace.aqi.fact_aqi_daily f
    JOIN workspace.aqi.dim_city c ON f.city_id = c.city_id
    JOIN workspace.aqi.dim_date d ON f.date_id = d.date_id
    WHERE d.year = 2019
    GROUP BY c.city, c.region
)
SELECT
    RANK() OVER (ORDER BY poor_days * 1.0 / days_with_data DESC) AS rnk,
    city,
    region,
    days_with_data,
    poor_days,
    ROUND(poor_days * 100.0 / days_with_data, 1) AS pct_poor_days,
    avg_aqi
FROM city_2019
WHERE days_with_data >= 300
ORDER BY rnk;