--delete from health_nav.core.master_table;

create or replace TABLE HEALTH_NAV.CORE.MASTER_TABLE (
	HOSPITAL_ID VARCHAR(16777216),
	INSURANCE_PROVIDER_ID NUMBER(38,0),
	INSURANCE_PLAN_ID NUMBER(38,0),
	CODE VARCHAR(16777216),
	STANDARD_CHARGE_DOLLAR NUMBER(38,0),
	MAXIMUM_CHARGE NUMBER(38,0),
	MINIMUM_CHARGE NUMBER(38,0),
	METHODOLOGY VARCHAR(16777216),
	constraint FK_PARENT foreign key (HOSPITAL_ID) references HEALTH_NAV.CORE.HOSPITAL_DATA(HOSPITAL_ID),
	constraint FK_PARENT1 foreign key (INSURANCE_PROVIDER_ID) references HEALTH_NAV.CORE.INSURANCE_PROVIDERS(INSURANCE_PROVIDER_ID),
	constraint FK_PARENT2 foreign key (INSURANCE_PLAN_ID) references HEALTH_NAV.CORE.INSURANCE_PLANS(INSURANCE_PLAN_ID),
	constraint FK_PARENT3 foreign key (CODE) references HEALTH_NAV.CORE.SERVICE_CODES(CODE)
);

------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,          
    cmc.minimum_charge,          
    hd.hospital_id                
FROM health_nav.core.carolinas_medical_center cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = 'H0071';
-----------------------------------st rose dominican rose de lima
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                     
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id                
FROM health_nav.core.st_rose_dominican_hospital_rose_de_lima cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = '659-HOS-40';

--------------------------------------st rose dominican siena
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id               
FROM health_nav.core.st_rose_dominican_hospital_siena cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = '2969-HOS-41';

--------------------------------------alexius medical center
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,          
    cmc.minimum_charge,          
    hd.hospital_id               
FROM health_nav.core.alexius_medical_center cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = '0004994';

--------------------------------------charlotte mecklenburg
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id                
FROM health_nav.core.CHARLOTTE_MECKLENBURG cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = 'H0082';
-----------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,           
    cmc.minimum_charge,          
    hd.hospital_id                
FROM health_nav.core.NOVANT_HEALTH_MINT_HILL_MEDICAL_CENTER cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = 'H0290';
-----------------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                     
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id               
FROM health_nav.core.NOVANT_HEALTH_BALLANTYNE_MEDICAL_CENTER cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = 'H0292';
-------------------------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                     
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,          
    cmc.minimum_charge,           
    hd.hospital_id               
FROM health_nav.core.NOVANT_HEALTH_CHARLOTTE_ORTHOPEDIC_HOSPITAL cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = 'H0010';
-----------------------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,          
    cmc.minimum_charge,           
    hd.hospital_id                
FROM health_nav.core.NOVANT_HEALTH_HUNTERSVILLE_MEDICAL_CENTER cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = 'H0010';
----------------------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id               
FROM health_nav.core.NOVANT_HEALTH_MATTHEWS_MEDICAL_CENTER cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = 'H0270';
---------------------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id               
FROM health_nav.core.NOVANT_HEALTH_PRESBYTERIAN_MEDICAL_CENTER cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = 'H0010';

---------------------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,          
    cmc.minimum_charge,           
    hd.hospital_id               
FROM health_nav.core.EDWARD_HOSPITAL cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = '003905';

---------------------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,  
    cmc.maximum_charge,          
    cmc.minimum_charge,           
    hd.hospital_id               
FROM health_nav.core.ELMHURST_MEMORIAL_HOSPITAL cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = '0005751';

---------------------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id               
FROM health_nav.core.LINDEN_OAKS_HOSPITAL cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = '0005058';
 --------------------------------------------------------   

INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,  
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id               
FROM health_nav.core.caromont_part1 cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = 'H0105';
---------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,  
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id                
FROM health_nav.core.caromont_part2 cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = 'H0105';
-------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                      
    cmc.standard_charge_dollar,   
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id                
FROM health_nav.core.touchette_regional_hospital cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = '4523';
----------------------------------------------------
INSERT INTO health_nav.core.master_table (
    insurance_provider_id,
    insurance_plan_id,
    code,
    standard_charge_dollar,
    maximum_charge,
    minimum_charge,
    hospital_id
)
SELECT DISTINCT
    ip.insurance_provider_id,
    pl.insurance_plan_id,
    sc.code,                     
    cmc.standard_charge_dollar,  
    cmc.maximum_charge,           
    cmc.minimum_charge,           
    hd.hospital_id               
FROM health_nav.core.ut_health_henderson cmc
JOIN health_nav.core.insurance_providers ip
    ON cmc.PAYER_NAME = ip.PAYER_NAME
JOIN health_nav.core.insurance_plans pl
    ON cmc.PLAN_NAME = pl.PLAN_NAME
JOIN health_nav.core.service_codes sc
    ON cmc.code = sc.code
JOIN health_nav.core.hospital_data hd
    ON hd.HOSPITAL_ID = '100428';
    
select count(*) from health_nav.core.master_table;

SELECT count(DISTINCT code)
FROM service_codes
group by code