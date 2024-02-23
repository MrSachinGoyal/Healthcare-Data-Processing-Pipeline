-- command to create table in GCP BigQuery
CREATE TABLE IF NOT EXISTS gcp_project_id.dataset_id.patients_data (
    patient_id STRING,
    age INT64,
    gender STRING,
    diagnosis_code STRING,
    disease STRING,
    diagnosis_date TIMESTAMP,
    age_group STRING,
    is_senior_citizen STRING,
    load_time TIMESTAMP
);

-- Sql queries

-- Calculate the gender ratio for each disease. This will help in identifying if a particular disease is more prevalent in a particular gender.
SELECT 
    disease,
    ROUND(COUNT(CASE WHEN gender = 'M' THEN 'male' END)/ COUNT(*), 2) AS male_ratio,
    ROUND(COUNT(CASE WHEN gender = 'F' THEN 'female' END) / COUNT(*), 2) AS female_ratio
FROM 
    `gcp_project_id.dataset_id.patients_data`
GROUP BY 
    disease;

-- Find the top 3 most common diseases in the dataset. This will help in identifying the most prevalent diseases.
SELECT
    disease,
    COUNT(*) AS patients_count
FROM
    `gcp_project_id.dataset_id.patients_data`
GROUP BY 
   disease
ORDER BY 
    patients_count DESC
LIMIT
    3;

-- Calculate the number of patients in each age category for each disease. This can help understand the age distribution of different diseases
SELECT  
    age_group,
    disease,
    COUNT(*) AS patients_count
FROM 
    `gcp_project_id.dataset_id.patients_data`
GROUP BY 
    age_group,
    disease;

-- calculate the number of cases of each disease for each day of the week to understand if there's a trend (more cases diagnosed on particular days)
SELECT
    EXTRACT(WEEK FROM (load_time)) AS week_num,
    disease
FROM 
    `gcp_project_id.dataset_id.patients_data`
GROUP BY
    week_num,
    disease







