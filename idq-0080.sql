/**************************************************************************************
--Description:  IDQ-0080 Rx claim without a single professional claim within the prior year

Percentages shown are in 3 buckets:
1) all Rx claims with Professional claims within a year,
2) all Rx claims with Professional claims not within the prior year,
3) all Rx claims with no professional claims. 
***************************************************************************************/

-- REPLACE <SCHEMA> WITH CLIENTS SCHEMA
-- REPLACE <POPULATION_ID> WITH CLIENTS POPULATION ID

WITH ANALYTICS_POPULATION_ID AS
(SELECT '<POPULATION_ID>' AS POPULATION_ID),

-- All Rx Claims within the last 2 years 
RXCLAIMS AS (
SELECT DISTINCT
       C.SOURCE_DESCRIPTION AS SOURCE_DESCRIPTION,
       C.POPULATION_ID AS POPULATION_ID,
       C.CLAIM_UID AS CLAIM_UID,
       C.FORM_TYPE AS FORM_TYPE,
       C.EMPI_ID AS EMPI_ID,
       C.INCURRED_FROM_DATE AS RX_INCURRED_FROM_DATE       
FROM <SCHEMA>.PH_F_CLAIM AS C
INNER JOIN ANALYTICS_POPULATION_ID AS P ON C.POPULATION_ID = P.POPULATION_ID
WHERE C.FORM_TYPE = 'Rx'
AND C.INCURRED_FROM_DATE BETWEEN CAST(GETDATE() - INTERVAL '2 Years' AS DATE) AND CAST(GETDATE() AS Date)
),

-- All Professional claims within the last 3 years. 
PCLAIMS AS (
SELECT DISTINCT
       C.SOURCE_DESCRIPTION AS SOURCE_DESCRIPTION,
       C.POPULATION_ID AS POPULATION_ID,
       C.CLAIM_UID AS CLAIM_UID,
       C.FORM_TYPE AS FORM_TYPE,
       C.EMPI_ID AS EMPI_ID,
       C.INCURRED_FROM_DATE AS P_INCURRED_FROM_DATE        
FROM <SCHEMA>.PH_F_CLAIM AS C
INNER JOIN ANALYTICS_POPULATION_ID AS P ON C.POPULATION_ID = P.POPULATION_ID
WHERE C.FORM_TYPE = 'P'
AND C.INCURRED_FROM_DATE BETWEEN CAST(GETDATE() - INTERVAL '3 Years' AS DATE) AND CAST(GETDATE() AS Date)
)

SELECT
      SOURCE_DESCRIPTION,
      POPULATION_ID,
      ROUND(SUM(WITHIN_YEAR/TOTAL), 2) AS WITHIN_YEAR_PERC,
      ROUND(SUM(NOT_WITHIN_YEAR/TOTAL), 2) AS NOT_WITHIN_YEAR_PERC,
      ROUND(SUM(NO_P_CLAIM/TOTAL), 2) AS NO_P_CLAIM_PERC,
      TOTAL AS TOTAL_P_CLAIMS
FROM
      (SELECT 
            SOURCE_DESCRIPTION,
            POPULATION_ID,
            SUM(CASE 
                      WHEN DATE_DIFF <= 365 AND DATE_DIFF >= 0 THEN 1
                      ELSE 0
                  END) AS WITHIN_YEAR,
            SUM(CASE 
                      WHEN DATE_DIFF > 365 THEN 1
                      ELSE 0
                  END) AS NOT_WITHIN_YEAR,
            SUM(CASE 
                      WHEN DATE_DIFF IS NULL THEN 1
                      ELSE 0
                  END) AS NO_P_CLAIM,
            COUNT(P_CLAIM_COUNT) AS TOTAL
      FROM        
        (SELECT -- All Rx claims with Professional Claims incurring within a year prior to the Rx claim.  
              RX.SOURCE_DESCRIPTION AS SOURCE_DESCRIPTION,
              RX.POPULATION_ID AS POPULATION_ID,
              RX.CLAIM_UID AS RX_CLAIM,      
              RX.EMPI_ID AS EMPI_ID,
              RX.RX_INCURRED_FROM_DATE AS RX_INCURRED_FROM_DATE,
              DATEDIFF(DAY, P_INCURRED_FROM_DATE, RX_INCURRED_FROM_DATE) AS DATE_DIFF,
              COUNT(P.CLAIM_UID) AS P_CLAIM_COUNT,
              CAST(GETDATE() - INTERVAL '3 Years' AS DATE) AS BEGINDATERANGE,
              CAST(GETDATE() AS Date) AS ENDDATERANGE
        FROM RXCLAIMS AS RX
        LEFT JOIN PCLAIMS AS P ON RX.EMPI_ID = P.EMPI_ID
                                   AND RX.POPULATION_ID = P.POPULATION_ID 
                                   AND RX.SOURCE_DESCRIPTION = P.SOURCE_DESCRIPTION
        WHERE DATEDIFF(DAY, P_INCURRED_FROM_DATE, RX_INCURRED_FROM_DATE) <= 365 -- filter to P claims within a year prior of the Rx_incurred_date
        AND DATEDIFF(DAY, P_INCURRED_FROM_DATE, RX_INCURRED_FROM_DATE) >= 0 
        GROUP BY 1,
                 2,
                 3,
                 4,
                 5,
                 6 
        UNION ALL 
        SELECT -- All Rx claims with Professional Claims incurring not within a year prior to the Rx claim. 
              RX.SOURCE_DESCRIPTION AS SOURCE_DESCRIPTION,
              RX.POPULATION_ID AS POPULATION_ID,
              RX.CLAIM_UID AS RX_CLAIM,      
              RX.EMPI_ID AS EMPI_ID,
              RX.RX_INCURRED_FROM_DATE,
              DATEDIFF(DAY, P_INCURRED_FROM_DATE, RX_INCURRED_FROM_DATE) AS DATE_DIFF,
              COUNT(P.CLAIM_UID) AS P_CLAIM_COUNT,
              CAST(GETDATE() - INTERVAL '3 Years' AS DATE) AS BEGINDATERANGE,
              CAST(GETDATE() AS Date) AS ENDDATERANGE
        FROM RXCLAIMS AS RX
        LEFT JOIN PCLAIMS AS P ON RX.EMPI_ID = P.EMPI_ID
                                   AND RX.POPULATION_ID = P.POPULATION_ID 
                                   AND RX.SOURCE_DESCRIPTION = P.SOURCE_DESCRIPTION
        WHERE DATEDIFF(DAY, P_INCURRED_FROM_DATE, RX_INCURRED_FROM_DATE) > 365 -- filter to P claims greater than a year prior of the Rx_incurred_date.
        GROUP BY 1,
                 2,
                 3,
                 4,
                 5,
                 6
        UNION ALL
        SELECT -- All Rx claims with no Professional Claims. 
              RX.SOURCE_DESCRIPTION AS SOURCE_DESCRIPTION,
              RX.POPULATION_ID AS POPULATION_ID,
              RX.CLAIM_UID AS RX_CLAIM,      
              RX.EMPI_ID AS EMPI_ID,
              RX.RX_INCURRED_FROM_DATE,
              DATEDIFF(DAY, P_INCURRED_FROM_DATE, RX_INCURRED_FROM_DATE) AS DATE_DIFF,
              COUNT(P.CLAIM_UID) AS P_CLAIM_COUNT,
              CAST(GETDATE() - INTERVAL '3 Years' AS DATE) AS BEGINDATERANGE,
              CAST(GETDATE() AS Date) AS ENDDATERANGE
        FROM RXCLAIMS AS RX
        LEFT JOIN PCLAIMS AS P ON RX.EMPI_ID = P.EMPI_ID
                                   AND RX.POPULATION_ID = P.POPULATION_ID 
                                   AND RX.SOURCE_DESCRIPTION = P.SOURCE_DESCRIPTION
        WHERE DATEDIFF(DAY, P_INCURRED_FROM_DATE, RX_INCURRED_FROM_DATE) IS NULL -- filter to Rx claims with no P claim.
        GROUP BY 1,
                 2,
                 3,
                 4,
                 5,
                 6
        HAVING COUNT(P.CLAIM_UID) = 0 -- filter to Rx claims with no P claim.
      ) AS SUB1
      GROUP BY 1,
               2) AS SUB2
GROUP BY 1,
         2,
         6
