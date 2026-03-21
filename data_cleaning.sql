use world_layoffs;

select * from dirty_layoffs_dataset;

select count(*) from dirty_layoffs_dataset;


create table layoffs_staging_layer2 like dirty_layoffs_dataset;


select * from layoffs_staging_layer2;

insert layoffs_staging_layer2 
select * from dirty_layoffs_dataset;

desc layoffs_staging_layer2;

with find_duplicates as (
	select *, 
			row_number() over(partition by company, location, industry, total_laid_off,date) as row_num 
	from layoffs_staging_layer2
)
select count(*) from find_duplicates where row_num>1;

create table layoffs_staging_layer2_transform like layoffs_staging_layer2;


select * from layoffs_staging_layer2_transform;

alter table layoffs_staging_layer2_transform 
add column row_num int;

insert into layoffs_staging_layer2_transform
select *, 
			row_number() over(partition by company, location, industry, total_laid_off,date) as row_num 
	from layoffs_staging_layer2;
    
select * from layoffs_staging_layer2_transform where row_num >1;


select distinct(trim(company)), company from layoffs_staging_layer2_transform;


select distinct(company) from layoffs_staging_layer2_transform where company like 'Ama%';


set sql_safe_updates=0;

update layoffs_staging_layer2_transform
set company = 'Amazon'
where company like 'Ama%';

select distinct(company) from layoffs_staging_layer2_transform where company like 'Go%';

update layoffs_staging_layer2_transform
set company = 'Google'
where company like 'Go%';


select distinct(company) from layoffs_staging_layer2_transform;

select company from layoffs_staging_layer2_transform;

update layoffs_staging_layer2_transform
set company = 'Amazon'
where company like 'Ama%' or trim(company) like 'AMA%';

desc layoffs_staging_layer2_transform;

select distinct(industry) from layoffs_staging_layer2_transform;

update layoffs_staging_layer2_transform
set industry = 'Technology';

select distinct(location) from layoffs_staging_layer2_transform;

update layoffs_staging_layer2_transform
set location = 'New York'
where location like 'New%' or location like 'Ny%';

update layoffs_staging_layer2_transform
set location = 'San Francisco'
where location like 'San%' or location like 'sf%';


--- deleting date column as it is having no source and no data information properly 

alter table layoffs_staging_layer2_transform
drop column date;

select * from layoffs_staging_layer2_transform where company='Amazon' and location !='New York';

select * from layoffs_staging_layer2_transform where company = 'Amazon' and location = 'San Francisco';

select * from layoffs_staging_layer2 where company = 'Amazon' and location = 'San Francisco';

update layoffs_staging_layer2_transform 
set location = concat(location,' United States');

select distinct(location) from layoffs_staging_layer2_transform;

update layoffs_staging_layer2_transform 
set location =
case
	when location like 'New%' then 'New York, United States'
    when location like 'San%' then 'San Francisco, United States'
	else 'United States'
end;

select distinct(company) from layoffs_staging_layer2_transform;
select distinct(location) from layoffs_staging_layer2_transform;
select distinct(industry) from layoffs_staging_layer2_transform;

alter table layoffs_staging_layer2_transform
add column date varchar(255); 

update layoffs_staging_layer2_transform t1
join layoffs_staging_layer2 t2
on t1.company=t2.company
and t1.industry=t2.industry
set t1.date=t2.date;

select * from layoffs_staging_layer2_transform;

select distinct(total_laid_off) from layoffs_staging_layer2_transform;

update layoffs_staging_layer2_transform
set total_laid_off = 
CASE
	when total_laid_off like '100' then '100'
    when total_laid_off like '-50' then '50'
    when total_laid_off like 'one%' then '100'
    else total_laid_off='0'
end;


select * from layoffs_staging_layer2_transform where row_num>1;


desc layoffs_staging_layer2_transform;

alter table layoffs_staging_layer2_transform
drop column row_num;


alter table layoffs_staging_layer2_transform
drop column date;


select count(*) from layoffs_staging_layer2_transform;


create table staging2 like layoffs_staging_layer2_transform;


alter table staging2 add column row_num int;

insert staging2 
select *, row_number() over(partition by company,location,industry,total_laid_off) as row_num
from layoffs_staging_layer2_transform;

select * from staging2;


delete from staging2 where row_num>1;