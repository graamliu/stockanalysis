WITH 
cte_convert_time AS (
    SELECT 
        from_iso8601_timestamp(ts) as time_of_day,
        high,
        name
    FROM "2021"),
cte_datetime_join AS (
    SELECT 
        time_of_day,
        high,
        hour(time_of_day) AS by_hour
    FROM cte_convert_time),
cte_max_hourly_price AS (
    SELECT 
        hour(time_of_day) AS by_hour, 
        max(high) AS max_price, 
        name
    FROM cte_convert_time
    GROUP BY 1, 3)

SELECT 
    m.name, 
    m.max_price, 
    j.time_of_day, 
    m.by_hour 
FROM 
    cte_max_hourly_price AS m
    LEFT JOIN cte_datetime_join AS j
    ON m.max_price = j.high
    AND m.by_hour = j.by_hour
GROUP BY 1,2,3,4
ORDER BY 4 ASC, 2 ASC;






