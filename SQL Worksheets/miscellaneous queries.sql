select * from health_nav.core.NOVANT_HEALTH_MINT_HILL_MEDICAL_CENTER_CLEANED;

select count(*) from health_nav.core.novant_health_charlotte_orthopedic_hospital;

select * from master_table JOIN HOSPITAL_DATA AS HD ON MASTER_TABLE.HOSPITAL_ID = HD.HOSPITAL_ID WHERE CODE='83520' AND ZIPCODE LIKE '8%' ;

select * from health_nav.core.TOUCHETTE_REGIONAL_HOSPITAL where CODE_TYPE != 'CPT';

SELECT CODE, COUNT(DISTINCT HOSPITAL_ID) AS hospital_count
FROM master_table
GROUP BY CODE
HAVING COUNT(DISTINCT HOSPITAL_ID) > 1;

select hospital_ID from master_table where code='86804';

SELECT CODE
FROM master_table
WHERE HOSPITAL_ID IN ('H0071', 'H0290')  -- replace with your actual IDs
GROUP BY CODE
HAVING COUNT(DISTINCT HOSPITAL_ID) = 2;



SELECT count(*) FROM MASTER_TABLE;

select *  from master_table where standard_charge_dollar = 0.00;

select count(*),hospital_id  from master_table where standard_charge_dollar = 0.00 group by hospital_id;

select count(*) from CHARLOTTE_MECKLENBURG;

delete from master_table where standard_charge_dollar = 0.00;