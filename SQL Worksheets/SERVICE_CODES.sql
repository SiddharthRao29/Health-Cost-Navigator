create or replace TABLE HEALTH_NAV.CORE.SERVICE_CODES (
	CODE VARCHAR(16777216) NOT NULL,
	CODE_TYPE VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216),
	constraint CONSTRAINT_PK primary key (CODE)
);
MERGE INTO HEALTH_NAV.CORE.SERVICE_CODES AS target
USING (
    WITH codes AS (
        SELECT description, code, code_type FROM HEALTH_NAV.CORE.ST_ROSE_DOMINICAN_HOSPITAL_ROSE_DE_LIMA
        UNION ALL
        SELECT description, code, code_type FROM HEALTH_NAV.CORE.CAROLINAS_MEDICAL_CENTER
        UNION ALL
        SELECT description, code, code_type FROM HEALTH_NAV.CORE.ST_ROSE_DOMINICAN_HOSPITAL_SIENA
        UNION ALL
        SELECT description, code, code_type FROM HEALTH_NAV.CORE.ALEXIUS_MEDICAL_CENTER
        UNION ALL
        SELECT description, code, code_type FROM HEALTH_NAV.CORE.CHARLOTTE_MECKLENBURG
        UNION ALL
        SELECT description, code, code_type FROM NOVANT_HEALTH_MINT_HILL_MEDICAL_CENTER
        UNION ALL
        SELECT description, code, code_type FROM NOVANT_HEALTH_BALLANTYNE_MEDICAL_CENTER
        UNION ALL
        SELECT description, code, code_type FROM NOVANT_HEALTH_CHARLOTTE_ORTHOPEDIC_HOSPITAL
        UNION ALL
        SELECT description, code, code_type FROM NOVANT_HEALTH_HUNTERSVILLE_MEDICAL_CENTER
        UNION ALL
        SELECT description, code, code_type FROM NOVANT_HEALTH_MATTHEWS_MEDICAL_CENTER
        UNION ALL
        SELECT description, code, code_type FROM NOVANT_HEALTH_PRESBYTERIAN_MEDICAL_CENTER
        UNION ALL
        SELECT description, code, code_type FROM EDWARD_HOSPITAL
        UNION ALL
        SELECT description, code, code_type FROM ELMHURST_MEMORIAL_HOSPITAL
        UNION ALL
        SELECT description, code, code_type FROM LINDEN_OAKS_HOSPITAL
        UNION ALL
        SELECT description, code, code_type FROM CAROMONT_PART1
        UNION ALL
        SELECT description, code, code_type FROM CAROMONT_PART2
        UNION ALL
        SELECT description, code, code_type FROM TOUCHETTE_REGIONAL_HOSPITAL
        UNION ALL
        SELECT description, code, code_type FROM UT_HEALTH_HENDERSON
        
        -- Add more hospital tables if needed
    )
    SELECT DISTINCT code, code_type, description
    FROM codes
    WHERE code IS NOT NULL
) AS source
ON target.code = source.code
WHEN NOT MATCHED THEN
    INSERT (code, code_type, description)
    VALUES (source.code, source.code_type, source.description);


/*SELECT DISTINCT code,description
FROM service_codes
ORDER BY code;

SELECT count(DISTINCT code)
FROM service_codes;

SELECT count(DISTINCT code)
FROM service_codes
group by code
*/

