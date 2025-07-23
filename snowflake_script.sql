create or replace database linkedin;


CREATE or replace STAGE my_public_s3_stage
  URL = 's3://snowflake-lab-bucket/';


  LIST @my_public_s3_stage; 

  CREATE or replace FILE FORMAT my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'N/A')
  EMPTY_FIELD_AS_NULL = TRUE
  field_optionally_enclosed_by = '"';


  CREATE FILE FORMAT my_json_format
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE; -- Supprime le tableau externe si votre fichier JSON commence par `[...]`
  
  SHOW FILE FORMATS;

  CREATE or replace TABLE jobs_posting (
  job_id                  integer PRIMARY KEY,
  company_name            STRING ,
  title                   STRING NOT NULL,
  description             STRING,
  max_salary              STRING,
  med_salary             STRING,
  min_salary              STRING,
  pay_period              STRING,
  formatted_work_type     STRING,
  location                STRING,
  applies                 INTEGER DEFAULT 0,
  original_listed_time    TIMESTAMP,
  remote_allowed          BOOLEAN DEFAULT FALSE,
  views                   INTEGER DEFAULT 0,
  job_posting_url         STRING,
  application_url         STRING,
  application_type        STRING,
  expiry                  TIMESTAMP,
  closed_time             TIMESTAMP,
  formatted_experience_level STRING,
  skills_desc             STRING,
  listed_time             TIMESTAMP,
  posting_domain          STRING,
  sponsored               BOOLEAN DEFAULT FALSE,
  work_type               STRING,
  currency                STRING,
  compensation_type       STRING
);


CREATE TEMPORARY TABLE job_posting_raw (
  job_id                  integer,
  company_name            STRING,
  title                   STRING,
  description             STRING,
  max_salary              STRING, -- Charger les données brutes comme chaînes
  med_salary              STRING,
  min_salary              STRING,
  pay_period              STRING,
  formatted_work_type     STRING,
  location                STRING,
  applies                 STRING,
  original_listed_time    STRING, -- Charger les timestamps comme chaînes
  remote_allowed          STRING,
  views                   STRING,
  job_posting_url         STRING,
  application_url         STRING,
  application_type        STRING,
  expiry                  STRING,
  closed_time             STRING,
  formatted_experience_level STRING,
  skills_desc             STRING,
  listed_time             STRING,
  posting_domain          STRING,
  sponsored               STRING,
  work_type               STRING,
  currency                STRING,
  compensation_type       STRING
);


COPY INTO job_posting_raw
FROM '@"LINKEDIN"."PUBLIC"."MY_PUBLIC_S3_STAGE"/job_postings.csv'
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
ON_ERROR = 'CONTINUE';


select * from jobs_posting;


INSERT INTO jobs_posting (
  job_id,
  company_name,
  title,
  description,
  max_salary,
  med_salary,
  min_salary,
  pay_period,
  formatted_work_type,
  location,
  applies,
  original_listed_time,
  remote_allowed,
  views,
  job_posting_url,
  application_url,
  application_type,
  expiry,
  closed_time,
  formatted_experience_level,
  skills_desc,
  listed_time,
  posting_domain,
  sponsored,
  work_type,
  currency,
  compensation_type
)
SELECT
  job_id,
  company_name,
  title,
  description,
  SCALED_ROUND_INT_DIVIDE(TRY_CAST(max_salary AS FLOAT), 1000) AS max_salary, -- Transformation complexe
  TRY_CAST(med_salary AS FLOAT),
  TRY_CAST(min_salary AS FLOAT),
  pay_period,
  formatted_work_type,
  location,
  TRY_CAST(applies AS INTEGER),
  TO_TIMESTAMP(TRY_CAST(original_listed_time AS NUMBER) / 1000),
  TRY_CAST(remote_allowed AS BOOLEAN),
  TRY_CAST(views AS INTEGER),
  job_posting_url,
  application_url,
  application_type,
  TO_TIMESTAMP(TRY_CAST(expiry AS NUMBER) / 1000),
  TO_TIMESTAMP(TRY_CAST(closed_time AS NUMBER) / 1000),
  formatted_experience_level,
  skills_desc,
  TO_TIMESTAMP(TRY_CAST(listed_time AS NUMBER) / 1000),
  posting_domain,
  TRY_CAST(sponsored AS BOOLEAN),
  work_type,
  currency,
  compensation_type
FROM job_posting_raw;

SELECT  *
FROM jobs_posting;


CREATE OR REPLACE TABLE Benefits (
  job_id    integer,
  inferred  BOOLEAN,
  type      STRING
);

COPY INTO Benefits
FROM '@"LINKEDIN"."PUBLIC"."MY_PUBLIC_S3_STAGE"/benefits.csv'
FILE_FORMAT = (FORMAT_NAME = my_csv_format);


CREATE or replace TABLE Employee_counts (
  company_id      integer,          -- Identifiant unique de l'entreprise
  employee_count  INTEGER,         -- Nombre d'employés dans l'entreprise
  follower_count  INTEGER,         -- Nombre de followers sur LinkedIn
  time_recorded   TIMESTAMP        -- Horodatage de la collecte des données (en secondes depuis l'époque Unix)
);


CREATE or replace temporary TABLE Employee_counts_temp (
  company_id      integer,          -- Identifiant unique de l'entreprise
  employee_count  INTEGER,         -- Nombre d'employés dans l'entreprise
  follower_count  INTEGER,         -- Nombre de followers sur LinkedIn
  time_recorded   string        -- Horodatage de la collecte des données (en secondes depuis l'époque Unix)
);

COPY INTO Employee_counts_temp 
FROM '@"LINKEDIN"."PUBLIC"."MY_PUBLIC_S3_STAGE"/employee_counts.csv'
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

insert into EMPLOYEE_COUNTS (
company_id,
employee_count,
follower_count,
time_recorded
)
select
company_id,
employee_count,
follower_count,
 TO_TIMESTAMP(TRY_CAST(time_recorded AS NUMBER) / 1000) AS time_recorded 
from EMPLOYEE_COUNTS_TEMP;

select * from EMPLOYEE_COUNTS;


CREATE or replace TABLE Job_Skills (
  job_id     integer NOT NULL, -- Référence à la table jobs (clé primaire)
  skill_abr  STRING NOT NULL, -- Référence à la table skills (clé étrangère ou abréviation)
  PRIMARY KEY (job_id) -- Clé primaire composite
);

COPY INTO JOB_SKILLS 
FROM '@"LINKEDIN"."PUBLIC"."MY_PUBLIC_S3_STAGE"/job_skills.csv'
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

select * from job_skills;

CREATE or replace TABLE Companies (
  company_id      integer,          -- ID unique défini par LinkedIn
  name            STRING,          -- Nom de l'entreprise
  description     STRING,          -- Description de l'entreprise
  company_size    INTEGER,         -- Taille de l'entreprise (0 = plus petite, 7 = plus grande)
  state           STRING,          -- État du siège social
  country         STRING,          -- Pays du siège social
  city            STRING,          -- Ville du siège social
  zip_code        STRING,          -- Code postal du siège social
  address         STRING,          -- Adresse du siège social
  url             STRING           -- Lien vers la page LinkedIn de l'entreprise
);

COPY INTO Companies (
  company_id,
  name,
  description,
  company_size,
  state,
  country,
  city,
  zip_code,
  address,
  url
)
FROM (
  SELECT
    $1:company_id::integer AS company_id,
    $1:name::STRING AS name,
    $1:description::STRING AS description,
    $1:company_size::INTEGER AS company_size,
    $1:state::STRING AS state,
    $1:country::STRING AS country,
    $1:city::STRING AS city,
    $1:zip_code::STRING AS zip_code,
    $1:address::STRING AS address,
    $1:url::STRING AS url
  FROM '@"LINKEDIN"."PUBLIC"."MY_PUBLIC_S3_STAGE"/companies.json'
)
FILE_FORMAT = (FORMAT_NAME = my_json_format)
;

select * from companies;


CREATE or replace TABLE Company_industries (
  company_id integer PRIMARY KEY, -- Clé primaire unique
  industry   STRING NOT NULL     -- Identifiant de l'industrie
);

COPY INTO Company_industries (
 company_id,
 industry
  )
  from (
     select
     $1:company_id::integer as company_id,
     $1:industry::string as industry
     from '@"LINKEDIN"."PUBLIC"."MY_PUBLIC_S3_STAGE"/company_industries.json'
  )
file_format = (format_name = my_json_format);


select * from company_industries;

CREATE or replace TABLE Company_specialities (
  company_id integer PRIMARY KEY, -- Clé primaire unique
  speciality   STRING NOT NULL     -- Identifiant de l'industrie
);

COPY INTO Company_specialities (
 company_id,
  speciality
  )
  from (
     select
     $1:company_id::integer as company_id,
     $1: speciality::string as  speciality
     from '@"LINKEDIN"."PUBLIC"."MY_PUBLIC_S3_STAGE"/company_specialities.json'
  )
file_format = (format_name = my_json_format);

select * from Company_specialities;

CREATE or replace TABLE Job_Industries (
  job_id integer primary key , -- Clé primaire unique
  industry_id   integer not null     -- Identifiant de l'industrie
);

COPY INTO Job_Industries (
 job_id,
  industry_id
  )
  from (
     select
     $1:job_id::integer as job_id,
     $1: industry_id::integer as  industry_id
     from '@"LINKEDIN"."PUBLIC"."MY_PUBLIC_S3_STAGE"/job_industries.json'
  )
file_format = (format_name = my_json_format);

select * from JOB_INDUSTRIES;