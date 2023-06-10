USE enviornment;
SELECT * FROM cleaned_environment;

-- Write an SQL query to solve the given problem statement.
-- Find the average temperature recorded for each device.
-- The task is to calculate the average temperature recorded for each device in the dataset.
SELECT device_id,AVG(temperature) 
FROM cleaned_environment
GROUP BY device_id;

-- Write an SQL query to solve the given problem statement.
-- Retrieve the top 5 devices with the highest average carbon monoxide levels.
-- This task involves identifying the devices with the highest average carbon monoxide levels and retrieving the top 5 devices based on this metric.
SELECT device_id,AVG(carbon_monoxide) as highest
FROM cleaned_environment
GROUP BY device_id
ORDER BY highest DESC
LIMIT 5;

-- Write an SQL query to solve the given problem statement.
-- Calculate the average temperature recorded in the cleaned_environment table
--  The objective is to Determine the average temperature recorded in the cleaned_environment dataset.
SELECT * FROM cleaned_environment;
SELECT AVG(temperature) FROM cleaned_environment;

-- Write an SQL query to solve the given problem statement.
-- Find the timestamp and temperature of the highest recorded temperature for each device.
-- This task requires identifying the highest recorded temperature for each device and retrieving the corresponding timestamp and temperature values.
SELECT device_id,timestamp,MAX(temperature) as highest 
FROM cleaned_environment
GROUP BY device_id,timestamp
ORDER BY highest DESC;

-- Write an SQL query to solve the given problem statement.
-- Identify devices where the temperature has increased from the minimum recorded temperature to the maximum recorded temperature
-- The goal is to Identify devices where the temperature has increased from the minimum recorded temperature to the maximum recorded temperature
SELECT device_id
FROM cleaned_environment
GROUP BY device_id
HAVING MAX(temperature) > MIN(temperature);

-- Write an SQL query to solve the given problem statement.
-- Calculate the exponential moving average of temperature for each device limit to 10 devices.
-- Calculate the exponential moving average (EMA) of the temperature for each device. 
-- Retrieve the device ID, timestamp, temperature, and the EMA temperature for the first 10 devices from the 'cleaned_environment' table. 
-- The EMA temperature is calculated by partitioning the data based on the device ID, ordering it by the timestamp, and considering all preceding rows up to the current row
SELECT * FROM cleaned_environment;
SELECT device_id, timestamp, temperature, ema_temperature FROM ( SELECT device_id, timestamp, temperature, AVG(temperature) OVER (PARTITION BY device_id 
ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ema_temperature, ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp) AS row_num FROM cleaned_environment ) subquery 
WHERE row_num <= 10 ORDER BY device_id, timestamp LIMIT 10;

-- Write an SQL query to solve the given problem statement.
-- Find the timestamps and devices where carbon monoxide level exceeds the average carbon monoxide level of all devices.
-- The objective is to identify the timestamps and devices where the carbon monoxide level exceeds the average carbon monoxide level across all devices.

SELECT timestamp,device_id
FROM cleaned_environment
WHERE carbon_monoxide>(SELECT AVG(carbon_monoxide) FROM cleaned_environment);

-- Write an SQL query to solve the given problem statement.
-- Retrieve the devices with the highest average temperature recorded.
-- The objective is to identify the devices that have recorded the highest average temperature among all the devices in the dataset.
SELECT device_id,AVG(temperature) AS highest
FROM cleaned_environment
GROUP BY device_id
ORDER BY highest DESC;

SELECT * FROM cleaned_environment;

-- Write an SQL query to solve the given problem statement.
-- Calculate the average temperature for each hour of the day across all devices.
-- The goal is to calculate the average temperature for each hour of the day, considering data from all devices.
SELECT EXTRACT(HOUR FROM timestamp) AS hour_of_day, AVG(temperature) AS average_temperature
FROM cleaned_environment
GROUP BY EXTRACT(HOUR FROM timestamp)
ORDER BY hour_of_day;

-- Write an SQL query to solve the given problem statement.
-- Which device(s) in the cleaned environment dataset have recorded only a single distinct temperature value?
-- The objective is to identify device(s) in the cleaned environment dataset have recorded only a single distinct temperature value.
SELECT device_id  FROM 
cleaned_environment
GROUP BY device_id
HAVING count(DISTINCT temperature)=1;

-- Write an SQL query to solve the given problem statement.
-- Find the devices with the highest humidity levels.
-- The objective is to identify the devices that have recorded the highest humidity levels.

SELECT device_id,MAX(humidity) AS highest
FROM cleaned_environment
GROUP BY device_id
ORDER BY highest DESC;

-- Write an SQL query to solve the given problem statement.
-- Calculate the average temperature for each device, excluding outliers (temperatures beyond 3 standard deviations).
-- This task requires calculating the average temperature for each device while excluding outliers, which are temperatures beyond 3 standard deviations from the mean.
SELECT device_id, AVG(temperature) AS average_temperature FROM cleaned_environment 
WHERE temperature BETWEEN (SELECT AVG(temperature) - 3 * STDDEV(temperature) FROM cleaned_environment) AND (SELECT AVG(temperature) + 3 * STDDEV(temperature) FROM cleaned_environment) 
GROUP BY device_id;

-- Write an SQL query to solve the given problem statement.
-- Retrieve the devices that have experienced a sudden change in humidity (greater than 50% difference) within a 30-minute window.
-- The goal is to identify devices that have undergone a sudden change in humidity, where the difference is greater than 50%, within a 30-minute time window.

SELECT table1.device_id, table1.timestamp, table1.humidity
FROM
(SELECT device_id, timestamp,
humidity,
LAG(humidity,1) OVER (
PARTITION BY device_id
ORDER BY timestamp),
(humidity - (LAG(humidity,1) OVER (
PARTITION BY device_id
ORDER BY timestamp))) diff,
ABS((humidity - (LAG(humidity,1) OVER (
PARTITION BY device_id
ORDER BY timestamp)))*100) c1
FROM `cleaned_environment`) table1
WHERE table1.c1 > 50;

-- Write an SQL query to solve the given problem statement.
-- Find the average temperature for each device during weekdays and weekends separately.
-- This task involves calculating the average temperature for each device separately for weekdays and weekends.
SELECT device_id, 
       CASE WHEN DAYOFWEEK(timestamp) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type, 
       AVG(temperature) AS average_temperature 
FROM cleaned_environment 
GROUP BY device_id, day_type;

-- Write an SQL query to solve the given problem statement.
-- Calculate the cumulative sum of temperature for each device, ordered by timestamp limit to 10.
-- The objective is to calculate the cumulative sum of temperature for each device, considering the records ordered by timestamp limit to 10

SELECT device_id, timestamp, temperature, SUM(temperature) OVER (PARTITION BY device_id ORDER BY timestamp) AS cumulative_temperature 
FROM cleaned_environment LIMIT 10;
















