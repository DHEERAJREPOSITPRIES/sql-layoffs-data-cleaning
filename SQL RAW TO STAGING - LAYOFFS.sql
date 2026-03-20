--- Data Clearning 

use world_layoffs;

SELECT * FROM Layoffs;

-- 1. Remove Duplicates 
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove columns and rows unnecessary


CREATE TABLE layoffs_staging 
like layoffs_raw;

select * from layoffs_staging;

insert layoffs_staging 
select * from layoffs_raw;


select *,
	row_number() over(
    partition by company,industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;


-- know all columns of the layoffs staging for the partition
desc layoffs_staging;


with duplicates_cte as (
select *,
	row_number() over(
    partition by company,location,industry,total_laid_off,percentage_laid_off,'date'
,stage
,country
,funds_raised_millions) as row_num
from layoffs_staging)
select * 
from duplicates_cte where row_num >1 ;


-- checked one of the column of the duplicate rows and reason of duplication

create view casper as select *,	row_number() over(
    partition by company,location,industry,total_laid_off,percentage_laid_off,date
,stage
,country
,funds_raised_millions order by (select null)) as row_num from layoffs_staging where company = 'Casper';

desc casper;

select * from casper;

-- found duplicates from casper: 

with duplicates_cte as (
select *,
	row_number() over(
    partition by company,location,industry,total_laid_off,percentage_laid_off,'date'
,stage
,country
,funds_raised_millions) as row_num
from layoffs_staging)
select * 
from duplicates_cte where row_num >1 ;


-- creating a table with row_num for manipulation

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




select * from layoffs_staging2;


insert into layoffs_staging2 
select *,
	row_number() over(
    partition by company,location,industry,total_laid_off,percentage_laid_off,'date'
,stage
,country
,funds_raised_millions) as row_num
from layoffs_staging;


select * from layoffs_staging2 where row_num>1;


-- as row_num is not a key column i am using safe updates
set sql_safe_updates=0;

delete from layoffs_staging2 where row_num>1;

set sql_safe_updates=1;

select * from layoffs_staging2;





-- 02. standardizing data  : finding issues in your data and fix it

select company,trim(company) from layoffs_staging2;

update layoffs_staging2
set company = trim(company);


select distinct industry 
from layoffs_staging2 
order by 1;

select *
from layoffs_staging2
where industry like 'crypto%';


set sql_safe_updates = 0;


update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%';

set sql_safe_updates = 1;

select * from layoffs_staging2;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

select * 
from layoffs_stagingf2
where country like 'United States%';

set sql_safe_updates=0;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United states%';

set sql_safe_updates=1;

select distinct country 
from layoffs_staging2
order by 1;

-- change date to date datatype as it is text previously

select date,
str_to_date(date, '%m/%d/%Y') as date_update 
from layoffs_staging2;

set sql_safe_updates=0;

update layoffs_staging2
set date=str_to_date(date, '%m/%d/%Y');


select date
from layoffs_staging2;


alter table layoffs_staging2 
modify column date date;

-- date is changed to date and formated perfeclty 

select * from layoffs_staging2;


-- 03. handling null values

 
select *
from layoffs_staging2
where industry is null 
or industry = ''; 


select *
from layoffs_staging2
where  company = 'Airbnb';


update layoffs_staging2
set industry = null 
where industry = '';


select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2 
	on t1.company=t2.company
    and t1.location=t2.location
where (t1.industry is null or t1.industry= '')
and t2.industry is not null;



UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
   AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;    
    
    
UPDATE layoffs_staging2 t1
JOIN (
    SELECT company, location, MAX(industry) AS industry
    FROM layoffs_staging2
    WHERE industry IS NOT NULL
    GROUP BY company, location
) t2
ON t1.company = t2.company
AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL;
    

SELECT t1.company, t1.location, t1.industry AS t1_industry,
       t2.industry AS t2_industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
 AND t1.location = t2.location
WHERE t1.company = 'Airbnb'
  AND t1.industry IS NULL
  AND t2.industry IS NOT NULL;
  
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON LOWER(TRIM(t1.company)) = LOWER(TRIM(t2.company))
 AND LOWER(TRIM(t1.location)) = LOWER(TRIM(t2.location))
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;
  
  
  
SELECT company, location, LENGTH(location)
FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'
AND industry IS NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE TRIM(industry) = '';


UPDATE layoffs_staging2 t1
JOIN (
    SELECT 
        LOWER(TRIM(company)) AS company,
        LOWER(TRIM(location)) AS location,
        MAX(industry) AS industry
    FROM layoffs_staging2
    WHERE industry IS NOT NULL
    GROUP BY LOWER(TRIM(company)), LOWER(TRIM(location))
) t2
ON LOWER(TRIM(t1.company)) = t2.company
AND LOWER(TRIM(t1.location)) = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL;

select *
from layoffs_staging2
where  company like 'Bally%';



select * 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

delete from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

select count(*) from layoffs_staging2;

alter table layoffs_staging2 
drop column row_num;

select count(*) from layoffs_staging;
