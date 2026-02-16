CREATE TABLE base_companies AS
SELECT
    _id."$oid" as company_id,
    name,
    permalink,
    category_code,
    homepage_url,
    twitter_username,
    number_of_employees,
    founded_year,
    founded_month,
    founded_day,
    deadpooled_year,
    description,

    CASE
        WHEN total_money_raised IS NULL OR total_money_raised = '' THEN 0
        WHEN total_money_raised = '$0' THEN 0
        WHEN UPPER(total_money_raised) LIKE '%B' THEN
            CAST(REGEXP_REPLACE(total_money_raised, '[^0-9.]', '', 'g') AS DOUBLE) * 1000000000
        WHEN UPPER(total_money_raised) LIKE '%M' THEN
            CAST(REGEXP_REPLACE(total_money_raised, '[^0-9.]', '', 'g') AS DOUBLE) * 1000000
        WHEN UPPER(total_money_raised) LIKE '%K' THEN
            CAST(REGEXP_REPLACE(total_money_raised, '[^0-9.]', '', 'g') AS DOUBLE) * 1000
        ELSE
            CAST(REGEXP_REPLACE(total_money_raised, '[^0-9.]', '', 'g') AS DOUBLE)
    END as total_money_raised_usd,

    total_money_raised as total_money_raised_original,
    email_address,
    phone_number,
    tag_list,
    created_at."$date" as created_at_timestamp,
    updated_at
FROM read_json('C:\Users\Asus\Downloads\companies.json', auto_detect=true);

SELECT * FROM base_companies;

CREATE TABLE offices_unnested AS (
    SELECT
        _id."$oid" as company_id,
        name as company_name,
        UNNEST(offices, recursive:=true)
    FROM read_json('C:\Users\Asus\Downloads\companies.json', auto_detect=true)
    WHERE offices IS NOT NULL
);

CREATE TABLE funding_unnested AS (
    SELECT
        _id."$oid" as company_id,
        name as company_name,
        UNNEST(funding_rounds, recursive:=true)
    FROM read_json('C:\Users\Asus\Downloads\companies.json', auto_detect=true)
    WHERE funding_rounds IS NOT NULL AND len(funding_rounds) > 0
);

CREATE TABLE relationships_unnested AS (
    SELECT
        _id."$oid" as company_id,
        name as company_name,
        UNNEST(relationships, recursive:=true)
    FROM read_json('C:\Users\Asus\Downloads\companies.json', auto_detect=true)
    WHERE relationships IS NOT NULL AND len(relationships) > 0
);

CREATE TABLE competitors_unnested AS (
    SELECT
        _id."$oid" as company_id,
        name as company_name,
        UNNEST(competitions, recursive:=true)
    FROM read_json('C:\Users\Asus\Downloads\companies.json', auto_detect=true)
    WHERE competitions IS NOT NULL AND len(competitions) > 0
);

CREATE TABLE products_unnested AS (
    SELECT
        _id."$oid" as company_id,
        name as company_name,
        UNNEST(products, recursive:=true)
    FROM read_json('C:\Users\Asus\Downloads\companies.json', auto_detect=true)
    WHERE products IS NOT NULL AND len(products) > 0
);

SELECT
    company_id,
    company_name,
    city,
    state_code,
    country_code,
    address1,
    address2,
    zip_code,
    latitude,
    longitude,
    description as office_description
FROM offices_unnested;

SELECT
    company_id,
    company_name,
    round_code,
    raised_amount,
    raised_currency_code,
    funded_year,
    funded_month,
    funded_day,
    source_url
FROM funding_unnested;

SELECT
    company_id,
    company_name,
    title,
    is_past,
    first_name,
    last_name,
    permalink
FROM relationships_unnested;

SELECT
    company_id,
    company_name,
    name as competitor_name,
    permalink as competitor_permalink
FROM competitors_unnested;

SELECT
    company_id,
    company_name,
    name as product_name,
    permalink as product_permalink
FROM products_unnested;

WITH per_relationship AS (
  SELECT
    bc.company_id,
    bc.name AS company_name,
    bc.total_money_raised_usd,

    COUNT(*) FILTER (WHERE r.is_past = false) OVER (PARTITION BY bc.company_id) AS current_employees,
    COUNT(*) FILTER (WHERE r.is_past = true)  OVER (PARTITION BY bc.company_id) AS former_employees,
    COUNT(*)                                   OVER (PARTITION BY bc.company_id) AS total_employees_ever
  FROM relationships_unnested r
  JOIN base_companies bc ON r.company_id = bc.company_id
),
per_company AS (
  SELECT DISTINCT
    company_id,
    company_name,
    total_money_raised_usd,
    current_employees,
    former_employees,
    current_employees - former_employees AS hiring_balance,
    total_employees_ever
  FROM per_relationship
)
SELECT
  company_name,
  total_money_raised_usd,
  current_employees,
  former_employees,
  hiring_balance,
  total_employees_ever
FROM per_company
WHERE total_employees_ever >= 3
ORDER BY hiring_balance DESC
LIMIT 10; -- інсайт - треба йти прицювати в твіттер


WITH per_row AS (
  SELECT
    r.title AS position,

    COUNT(*) FILTER (WHERE r.is_past = false) OVER (PARTITION BY r.title) AS current_openings,
    COUNT(*) FILTER (WHERE r.is_past = true)  OVER (PARTITION BY r.title) AS former_count,
    COUNT(*)                                   OVER (PARTITION BY r.title) AS total_rows,

    COUNT(DISTINCT r.company_id) FILTER (WHERE r.is_past = false)
      OVER (PARTITION BY r.title) AS companies_hiring_now,

    ROUND(
      (AVG(bc.total_money_raised_usd) FILTER (WHERE r.is_past = false)
        OVER (PARTITION BY r.title)) / 1000000,
      2
    ) AS avg_funding_millions,

    STRING_AGG(DISTINCT bc.name, ', ') FILTER (WHERE r.is_past = false)
      OVER (PARTITION BY r.title) AS companies_with_openings
  FROM relationships_unnested r
  JOIN base_companies bc ON r.company_id = bc.company_id
  WHERE r.title IS NOT NULL AND r.title <> ''
),
per_position AS (
  SELECT DISTINCT
    position,
    current_openings,
    former_count,
    (current_openings - former_count) AS hiring_demand,
    companies_hiring_now,
    avg_funding_millions,
    companies_with_openings,
    total_rows
  FROM per_row
),
ranked AS (
  SELECT
    position,
    current_openings,
    former_count,
    hiring_demand,
    companies_hiring_now,
    avg_funding_millions,
    companies_with_openings,

    RANK() OVER (ORDER BY hiring_demand DESC) AS demand_rank,

    ROUND(
      (hiring_demand /
        NULLIF(former_count, 0)) * 100,
      1
    ) AS growth_rate_percent,

    total_rows
  FROM per_position
)
SELECT
  position,
  current_openings,
  former_count,
  hiring_demand,
  companies_hiring_now,
  avg_funding_millions,
  companies_with_openings,
  demand_rank,
  growth_rate_percent
FROM ranked
WHERE total_rows >= 3
-- AND position = 'Data Analyst'
ORDER BY hiring_demand DESC
LIMIT 10; -- інсайт - треба ставати бєшником, і йти на CEO


COPY (
WITH per_relationship AS (
  SELECT
    bc.company_id,
    bc.name AS company_name,
    bc.total_money_raised_usd,

    COUNT(*) FILTER (WHERE r.is_past = false) OVER (PARTITION BY bc.company_id) AS current_employees,
    COUNT(*) FILTER (WHERE r.is_past = true)  OVER (PARTITION BY bc.company_id) AS former_employees,
    COUNT(*)                                   OVER (PARTITION BY bc.company_id) AS total_employees_ever
  FROM relationships_unnested r
  JOIN base_companies bc ON r.company_id = bc.company_id
),
per_company AS (
  SELECT DISTINCT
    company_id,
    company_name,
    total_money_raised_usd,
    current_employees,
    former_employees,
    current_employees - former_employees AS hiring_balance,
    total_employees_ever
  FROM per_relationship
)
SELECT
  company_name,
  total_money_raised_usd,
  current_employees,
  former_employees,
  hiring_balance,
  total_employees_ever
FROM per_company
WHERE total_employees_ever >= 3
ORDER BY hiring_balance DESC
LIMIT 10
) TO 'C:\Users\Asus\Downloads\top10_companies.csv'
WITH (FORMAT CSV, HEADER TRUE);


COPY (
WITH per_row AS (
  SELECT
    r.title AS position,

    COUNT(*) FILTER (WHERE r.is_past = false) OVER (PARTITION BY r.title) AS current_openings,
    COUNT(*) FILTER (WHERE r.is_past = true)  OVER (PARTITION BY r.title) AS former_count,
    COUNT(*)                                   OVER (PARTITION BY r.title) AS total_rows,

    COUNT(DISTINCT r.company_id) FILTER (WHERE r.is_past = false)
      OVER (PARTITION BY r.title) AS companies_hiring_now,

    ROUND(
      (AVG(bc.total_money_raised_usd) FILTER (WHERE r.is_past = false)
        OVER (PARTITION BY r.title)) / 1000000,
      2
    ) AS avg_funding_millions,

    STRING_AGG(DISTINCT bc.name, ', ') FILTER (WHERE r.is_past = false)
      OVER (PARTITION BY r.title) AS companies_with_openings
  FROM relationships_unnested r
  JOIN base_companies bc ON r.company_id = bc.company_id
  WHERE r.title IS NOT NULL AND r.title <> ''
),
per_position AS (
  SELECT DISTINCT
    position,
    current_openings,
    former_count,
    (current_openings - former_count) AS hiring_demand,
    companies_hiring_now,
    avg_funding_millions,
    companies_with_openings,
    total_rows
  FROM per_row
),
ranked AS (
  SELECT
    position,
    current_openings,
    former_count,
    hiring_demand,
    companies_hiring_now,
    avg_funding_millions,
    companies_with_openings,

    RANK() OVER (ORDER BY hiring_demand DESC) AS demand_rank,

    ROUND(
      (hiring_demand::DOUBLE PRECISION /
        NULLIF(former_count, 0)) * 100,
      1
    ) AS growth_rate_percent,

    total_rows
  FROM per_position
)
SELECT
  position,
  current_openings,
  former_count,
  hiring_demand,
  companies_hiring_now,
  avg_funding_millions,
  companies_with_openings,
  demand_rank,
  growth_rate_percent
FROM ranked
WHERE total_rows >= 3
-- AND position = 'Data Analyst'
ORDER BY hiring_demand DESC
LIMIT 10
) TO 'C:\Users\Asus\Downloads\top10_positions.csv'
WITH (FORMAT CSV, HEADER TRUE);
