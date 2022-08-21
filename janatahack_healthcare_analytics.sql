-- Load hospital case dataset
SELECT TOP 10 *
FROM Healthcare..hospital_case;

-- Load hospital patient dataset
SELECT TOP 10 *
FROM Healthcare..hospital_patient;

-- Looking total department
SELECT COUNT(DISTINCT Department) AS total_department
FROM Healthcare..hospital_case;

-- Looking differences of department
SELECT DISTINCT
	   Department
FROM Healthcare..hospital_case;

-- Looking department count
SELECT Department,
	   COUNT(case_id) AS department_count
FROM Healthcare..hospital_case
GROUP BY Department
ORDER BY department_count DESC;

-- Looking total ward type
SELECT COUNT(DISTINCT Ward_Type) AS total_ward_type
FROM Healthcare..hospital_case;

-- Looking differences of ward type
SELECT DISTINCT
	   Ward_Type
FROM Healthcare..hospital_case;

-- Looking ward type count
SELECT Ward_Type, 
       COUNT(case_id) AS ward_type_count
FROM Healthcare..hospital_case
GROUP BY Ward_Type
ORDER BY ward_type_count DESC;

-- Looking total type admission
SELECT COUNT(DISTINCT [Type of Admission]) AS total_type_admission
FROM Healthcare..hospital_patient;

-- Looking differences of type admission
SELECT DISTINCT 
       [Type of Admission]
FROM Healthcare..hospital_patient;

-- Looking type admission count
SELECT [Type of Admission], 
       COUNT(PatientId) AS type_admission_count
FROM Healthcare..hospital_patient
GROUP BY [Type of Admission]
ORDER BY type_admission_count DESC;

-- Looking total severity illness
SELECT COUNT(DISTINCT [Severity of Illness]) AS total_severity_illness
FROM Healthcare..hospital_patient;

-- Looking differences of severity illness
SELECT DISTINCT
	   [Severity of Illness]
FROM Healthcare..hospital_patient;

-- Looking severity illness count
SELECT [Severity of Illness], 
       COUNT(PatientId) AS severity_illness_count
FROM Healthcare..hospital_patient
GROUP BY [Severity of Illness]
ORDER BY severity_illness_count DESC;

-- Looking average patient and average admission deposit based on type admission on trauma
SELECT cas.Department, 
       cas.Ward_Type, 
       pat.[Severity of Illness] AS Severity_Ilness, 
       pat.Age, 
       pat.Stay, 
       ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit,
	   ROUND(AVG(pat.patientid), 2) AS avg_patient
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
WHERE [Type of Admission] = 'Trauma'
GROUP BY Department, 
         Ward_Type, 
         [Severity of Illness], 
         Age, 
         Stay
ORDER BY avg_patient DESC;

-- Looking average patient and average admission deposit based on type admission on emergency
SELECT cas.Department, 
       cas.Ward_Type, 
       pat.[Severity of Illness] AS Severity_Ilness, 
       pat.Age, 
       pat.Stay,   
       ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit,
	   ROUND(AVG(pat.patientid), 2) AS avg_patient
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
WHERE [Type of Admission] = 'Emergency'
GROUP BY Department, 
         Ward_Type, 
         [Severity of Illness], 
         Age, 
         Stay
ORDER BY avg_patient DESC;

-- Looking average patient and average admission deposit based on type admission on urgent
SELECT cas.Department, 
       cas.Ward_Type, 
       pat.[Severity of Illness] AS Severity_Ilness, 
       pat.Age, 
       pat.Stay,   
       ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit,
	   ROUND(AVG(pat.patientid), 2) AS avg_patient
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
WHERE [Type of Admission] = 'Urgent'
GROUP BY Department, 
         Ward_Type, 
         [Severity of Illness], 
         Age, 
         Stay
ORDER BY avg_patient DESC;

-- Looking total cases and average admission deposit based on severity illness on extreme
SELECT cas.Department,
	   cas.Ward_Type,
	   pat.[Type of Admission] AS Type_Admission,
	   pat.Age,
	   pat.Stay,
	   ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit,
	   SUM(cas.case_id) AS total_cases
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
WHERE [Severity of Illness] = 'Extreme'
GROUP BY Department,
	     Ward_Type,
		 [Type of Admission],
		 Age,
		 Stay
ORDER BY total_cases DESC;

-- Looking total cases and average admission deposit based on severity illness on minor
SELECT cas.Department,
	   cas.Ward_Type,
	   pat.[Type of Admission] AS Type_Admission,
	   pat.Age,
	   pat.Stay,
	   ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit,
	   SUM(cas.case_id) AS total_cases
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
WHERE [Severity of Illness] = 'Minor'
GROUP BY Department,
	     Ward_Type,
		 [Type of Admission],
		 Age,
		 Stay
ORDER BY total_cases DESC;

-- Looking total cases and average admission deposit based on severity illness on moderate
SELECT cas.Department,
	   cas.Ward_Type,
	   pat.[Type of Admission] AS Type_Admission,
	   pat.Age,
	   pat.Stay,
	   ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit,
	   SUM(cas.case_id) AS total_cases
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
WHERE [Severity of Illness] = 'Moderate'
GROUP BY Department,
	     Ward_Type,
		 [Type of Admission],
		 Age,
		 Stay
ORDER BY total_cases DESC;

-- Looking total cases, average admission deposit and average patient based on age between 11 and 30
SELECT cas.Department, 
       cas.Ward_Type, 
       pat.[Type of Admission] AS Type_Admission, 
       pat.[Severity of Illness] AS Severity_Illness, 
       pat.Stay, 
       ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit, 
       ROUND(AVG(pat.patientid), 2) AS avg_patient, 
       SUM(cas.case_id) AS total_cases
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
WHERE Age IN('11-20', '21-30')
GROUP BY Department, 
         Ward_Type, 
         [Type of Admission], 
         [Severity of Illness], 
         Stay
ORDER BY total_cases DESC;

-- Looking total cases, average admission deposit and average patient based on age between 31 and 50
SELECT cas.Department, 
       cas.Ward_Type, 
       pat.[Type of Admission] AS Type_Admission, 
       pat.[Severity of Illness] AS Severity_Illness, 
       pat.Stay, 
       ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit, 
       ROUND(AVG(pat.patientid), 2) AS avg_patient, 
       SUM(cas.case_id) AS total_cases
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
WHERE Age IN('31-40', '41-50')
GROUP BY Department, 
         Ward_Type, 
         [Type of Admission], 
         [Severity of Illness], 
         Stay
ORDER BY total_cases DESC;

-- Looking total cases, average admission deposit and average patient based on age between 51 and 70
SELECT cas.Department, 
       cas.Ward_Type, 
       pat.[Type of Admission] AS Type_Admission, 
       pat.[Severity of Illness] AS Severity_Illness, 
       pat.Stay, 
       ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit, 
       ROUND(AVG(pat.patientid), 2) AS avg_patient, 
       SUM(cas.case_id) AS total_cases
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
WHERE Age IN('51-60', '61-70')
GROUP BY Department, 
         Ward_Type, 
         [Type of Admission], 
         [Severity of Illness], 
         Stay
ORDER BY total_cases DESC;

-- Looking total cases, average admission deposit and average patient based on stay more than 100 days
SELECT cas.Department, 
       cas.Ward_Type, 
       pat.[Type of Admission] AS Type_Admission, 
       pat.[Severity of Illness] AS Severity_Illness, 
       pat.Age, 
       ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit, 
       ROUND(AVG(pat.patientid), 2) AS avg_patient, 
       SUM(cas.case_id) AS total_cases
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
WHERE Stay = 'More than 100 Days'
GROUP BY Department, 
         Ward_Type, 
         [Type of Admission], 
         [Severity of Illness], 
         Age
ORDER BY total_cases DESC;

-- Looking maximum admission deposit based on department
WITH department_deposit_max
     AS (SELECT cas.Department, 
                pat.Admission_Deposit
         FROM Healthcare..hospital_patient pat
              INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid)
     SELECT *
     FROM department_deposit_max PIVOT(MAX(Admission_Deposit) FOR Department IN(anesthesia, 
                                                                                gynecology, 
                                                                                radiotheraphy, 
                                                                                surgery, 
                                                                                [TB & Chest disease])) AS department_deposit_max_pvt;
       
-- Looking minimum admission deposit based on department
WITH department_deposit_min
     AS (SELECT cas.Department, 
                pat.Admission_Deposit
         FROM Healthcare..hospital_patient pat
              INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid)
     SELECT *
     FROM department_deposit_min PIVOT(MIN(Admission_Deposit) FOR Department IN(anesthesia, 
                                                                                gynecology, 
                                                                                radiotheraphy, 
                                                                                surgery, 
                                                                                [TB & Chest disease])) AS department_deposit_min_pvt;

-- Looking total cases based on department
SELECT *
FROM
(
    SELECT Department, 
           case_id
    FROM Healthcare..hospital_case
) AS department_cases PIVOT(SUM(case_id) FOR Department IN(anesthesia, 
                                                           gynecology, 
                                                           radiotheraphy, 
                                                           surgery, 
                                                           [TB & Chest disease])) AS department_cases_pvt;

-- Looking average patient based on department
WITH department_patient_avg
     AS (SELECT cas.Department, 
                pat.patientid
         FROM Healthcare..hospital_patient pat
              INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid)
     SELECT *
     FROM department_patient_avg PIVOT(AVG(patientid) FOR Department IN(anesthesia, 
                                                                        gynecology, 
                                                                        radiotheraphy, 
                                                                        surgery, 
                                                                        [TB & Chest disease])) AS department_patient_avg_pvt;

-- Looking average admission, extra room, patient and total cases based on department, severity illness and type admission
SELECT cas.Department, 
       pat.[Severity of Illness] AS Severity_Illness, 
       pat.[Type of Admission] AS Type_Admission, 
       ROUND(AVG(cas.[Available Extra Rooms in Hospital]), 2) AS avg_extra_rooms, 
       SUM(cas.case_id) AS total_cases, 
       ROUND(AVG(pat.patientid), 2) AS avg_patient, 
       ROUND(AVG(pat.Admission_Deposit), 2) AS avg_admission_deposit
FROM Healthcare..hospital_patient pat
     INNER JOIN Healthcare..hospital_case cas ON pat.patientid = cas.patientid
GROUP BY Department, 
         [Severity of Illness], 
         [Type of Admission];