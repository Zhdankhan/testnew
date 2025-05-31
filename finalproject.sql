create database miniprogect;
use miniprogect;
CREATE TABLE customer_info (
    ID_client INT PRIMARY KEY,
    Total_amount DECIMAL(10,2),
    Gender VARCHAR(5),
    Age INT,
    Count_city INT,
    Response_communcation INT,
    Communication_3month INT,
    Tenure INT
);

CREATE TABLE transactions_info (
    date_new VARCHAR(10),  -- Временно как строка, позже преобразуем в DATE
    Id_check INT,
    ID_client INT,
    Count_products DECIMAL(10,2),
    Sum_payment DECIMAL(10,2)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customer_info.csv'
INTO TABLE customer_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@Id_client, @Total_amount, @Gender, @Age, @Count_city, @Response_communcation, @Communication_3month, @Tenure)
SET 
    Id_client = @Id_client,
    Total_amount = @Total_amount,
    Gender = @Gender,
    Age = NULLIF(@Age, ''),  -- Если пустое значение, заменяем на NULL
    Count_city = @Count_city,
    Response_communcation = @Response_communcation,
    Communication_3month = @Communication_3month,
    Tenure = @Tenure;
    
    select * from customer_info;
    
    LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions_info.csv'
INTO TABLE transactions_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@date_new, @Id_check, @ID_client, @Count_products, @Sum_payment)
SET 
    date_new = NULLIF(@date_new, ''),  
    Id_check = NULLIF(@Id_check, ''),  
    ID_client = NULLIF(@ID_client, ''),  
    Count_products = NULLIF(@Count_products, ''),  
    Sum_payment = NULLIF(@Sum_payment, '');
    
    
      select * from transactions_info;
      
-- 1. Создадим временную таблицу для клиентов с непрерывной историей за год
WITH MonthlyActivity AS (
    SELECT 
        ID_client, 
        DATE_FORMAT(STR_TO_DATE(date_new, '%d/%m/%Y'), '%Y-%m') AS txn_month
    FROM transactions_info
    WHERE STR_TO_DATE(date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY ID_client, txn_month
),
ActiveClients AS (
    SELECT ID_client
    FROM MonthlyActivity
    GROUP BY ID_client
    HAVING COUNT(DISTINCT txn_month) = 12
)
SELECT DISTINCT ID_client FROM ActiveClients;

-- 2. Средний чек за период
SELECT 
    AVG(Sum_payment) AS avg_check
FROM transactions_info
WHERE STR_TO_DATE(date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01';

-- 3. Средняя сумма покупок за месяц
SELECT 
    DATE_FORMAT(STR_TO_DATE(date_new, '%d/%m/%Y'), '%Y-%m') AS month, 
    AVG(Sum_payment) AS avg_monthly_spend
FROM transactions_info
WHERE STR_TO_DATE(date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY month;

-- 4. Количество всех операций по клиенту за период
SELECT 
    ID_client, 
    COUNT(*) AS total_transactions
FROM transactions_info
WHERE STR_TO_DATE(date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY ID_client;

-- 5. Средняя сумма чека в месяц
SELECT 
    DATE_FORMAT(STR_TO_DATE(date_new, '%d/%m/%Y'), '%Y-%m') AS month, 
    AVG(Sum_payment) AS avg_check_per_month
FROM transactions_info
WHERE STR_TO_DATE(date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY month;

-- 6. Среднее количество операций в месяц
SELECT 
    DATE_FORMAT(STR_TO_DATE(date_new, '%d/%m/%Y'), '%Y-%m') AS month, 
    COUNT(*) / COUNT(DISTINCT ID_client) AS avg_txn_per_client
FROM transactions_info
WHERE STR_TO_DATE(date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY month;

-- 7. Доля операций и сумм в месяц от общего
WITH Total AS (
    SELECT COUNT(*) AS total_txns, SUM(Sum_payment) AS total_amount
    FROM transactions_info
    WHERE STR_TO_DATE(date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01'
)
SELECT 
    DATE_FORMAT(STR_TO_DATE(date_new, '%d/%m/%Y'), '%Y-%m') AS month,
    COUNT(*) / (SELECT total_txns FROM Total) AS txn_share,
    SUM(Sum_payment) / (SELECT total_amount FROM Total) AS amount_share
FROM transactions_info
WHERE STR_TO_DATE(date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY month;

-- 8. % соотношение M/F/NA по месяцам
SELECT 
    DATE_FORMAT(STR_TO_DATE(t.date_new, '%d/%m/%Y'), '%Y-%m') AS month,
    c.Gender,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY DATE_FORMAT(STR_TO_DATE(t.date_new, '%d/%m/%Y'), '%Y-%m')) AS gender_share
FROM transactions_info t
JOIN customer_info c ON t.ID_client = c.ID_client
WHERE STR_TO_DATE(t.date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY month, c.Gender;

-- 9. Возрастные группы (шаг 10 лет)
WITH AgeGroups AS (
    SELECT 

        CASE 
            WHEN Age IS NULL THEN 'Unknown'
            WHEN Age < 20 THEN '10-19'
            WHEN Age < 30 THEN '20-29'
            WHEN Age < 40 THEN '30-39'
            WHEN Age < 50 THEN '40-49'
            WHEN Age < 60 THEN '50-59'
            ELSE '60+'
        END AS age_group,
        SUM(Sum_payment) AS total_spent,
        COUNT(*) AS total_txns
    FROM transactions_info t
    JOIN customer_info c ON t.ID_client = c.ID_client
    WHERE STR_TO_DATE(t.date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY age_group
)
SELECT * FROM AgeGroups;

-- 10. Поквартальные средние показатели
SELECT 
    QUARTER(STR_TO_DATE(date_new, '%d/%m/%Y')) AS quarter,
    AVG(Sum_payment) AS avg_spend_per_quarter,
    COUNT(*) / COUNT(DISTINCT ID_client) AS avg_txn_per_client
FROM transactions_info
WHERE STR_TO_DATE(date_new, '%d/%m/%Y') BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY quarter;
