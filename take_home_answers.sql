--Take note that many of the queries are limited by 5000 rows to reduce query times

--QUESTION: Which are the top 10 account ids by number of users?
SELECT
    account_id,
    COUNT(DISTINCT user_id) AS num_users
FROM
    tasks_used_da
GROUP BY
    account_id
ORDER BY
    Num_users DESC
LIMIT 10;

--QUESTION: Create a summary table at the account level that signals when an account is new (boolean).
--An account is new for the first day we see it run a task(s)

--first we need to get the most recent run date for each account
WITH first_run_date AS
(
    SELECT
        account_id,
        MIN(date) AS run_date
    FROM
        tasks_used_da
    GROUP BY
        account_id
)

--then join first_run_date with tasks_used_da and check if the t.date = run_date
SELECT
    t.date,
    t.account_id,
    SUM(t.sum_tasks_used) AS tasks,
    CASE WHEN t.date = run_date THEN TRUE
        ELSE FALSE END AS is_new
FROM
    tasks_used_da AS t
LEFT JOIN first_run_date AS rd ON 1=1
    AND rd.account_id = t.account_id
GROUP BY
    t.date,
    t.account_id,
    is_new
ORDER BY
    t.account_id,
    t.date
LIMIT 5000;



--QUESTION: Create a summary table at the account level. Add a column with the % difference in the
--number of tasks to the previous day
--QUESTION:Add another column with the moving average of the tasks run in the last 7 days for each
--account.

--first need to sum tasks per account and day
WITH sum_account_tasks_per_day AS
(
    SELECT
            date,
            account_id,
            CAST(SUM(sum_tasks_used) AS FLOAT) as tasks
    FROM
        tasks_used_da
    GROUP BY
        date, account_id
),

--calculate previous_day_tasks with window function
pd_tasks AS
(
     SELECT date,
             account_id,
             CONVERT(DECIMAL(18,3),tasks) AS tasks,
             CONVERT(DECIMAL(18,3), LAG(tasks)
                OVER (PARTITION BY account_id ORDER BY date)) AS previous_day_tasks
      FROM sum_account_tasks_per_day
)

--calculate pct_to_previous_day and moving average
SELECT
    date,
    account_id,
    tasks,
    ROUND(previous_day_tasks / NULLIF((tasks + previous_day_tasks), 0), 3) AS pct_to_previous_day,
    AVG(tasks) OVER (PARTITION BY account_id ORDER BY account_id,
        date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
        AS rolling_7_day_avg
FROM
    pd_tasks
ORDER BY
    account_id,
    date
LIMIT 5000;



--QUESTION: A lost account is an account with no tasks run on a given month. How many accounts
--did we lose (had no executed tasks) in February 2017?

--first grab all active february accounts
WITH active_feb_accounts AS
(
    SELECT
        account_id,
        SUM(sum_tasks_used) AS total_tasks
    FROM
        tasks_used_da
    GROUP BY
        account_id
    HAVING
        SUM(sum_tasks_used) > 0

)

--count all unique accounts that are not in the active february table
SELECT
    COUNT(DISTINCT account_id)
FROM
    tasks_used_da
WHERE
    account_id NOT IN (SELECT account_id FROM active_feb_accounts);




--QUESTION: (OPTIONAL) Create a visualization that represents the growth of new accounts in a way
--you would communicate to a peer or business stakeholder.
WITH first_run_date AS
(
    SELECT
        account_id,
        MIN(date) AS run_date
    FROM
        tasks_used_da
    GROUP BY
        account_id
),

--then join first_run_date with tasks_used_da and check if the t.date = run_date
new_accounts AS
(
         SELECT t.date,
                 t.account_id,
                 SUM(t.sum_tasks_used) AS tasks,
                 CASE
                     WHEN t.date = run_date THEN 1
                     ELSE 0
                     END AS is_new_count
          FROM tasks_used_da AS t
                   LEFT JOIN first_run_date AS rd ON 1 = 1
              AND rd.account_id = t.account_id
          GROUP BY t.date,
                   t.account_id,
                   is_new_count
          ORDER BY t.account_id,
                   t.date
)

--this query output was then exported to csv to be input for data studio visualization
SELECT
    date,
    SUM(is_new_count) as new_accounts
FROM
    new_accounts
GROUP BY
    date
ORDER BY
    date

