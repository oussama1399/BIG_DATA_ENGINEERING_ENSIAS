-- Script Pig pour l'analyse des employés
-- Chargement des données

-- Charger les employés
employees = LOAD '/user/root/input/employees.txt' USING PigStorage(',') AS (
    emp_id:int,
    name:chararray,
    gender:chararray,
    salary:int,
    dept_id:int,
    city:chararray,
    country:chararray
);

-- Charger les départements
departments = LOAD '/user/root/input/department.txt' USING PigStorage(',') AS (
    dept_id:int,
    dept_name:chararray
);

-- Nettoyer les espaces blancs dans les données
employees_clean = FOREACH employees GENERATE
    emp_id,
    TRIM(name) AS name,
    TRIM(gender) AS gender,
    salary,
    dept_id,
    TRIM(city) AS city,
    TRIM(country) AS country;

departments_clean = FOREACH departments GENERATE
    dept_id,
    TRIM(dept_name) AS dept_name;

-- ========================================
-- Question 1: Salaire moyen par département
-- ========================================
emp_by_dept = GROUP employees_clean BY dept_id;
avg_salary_by_dept = FOREACH emp_by_dept GENERATE
    group AS dept_id,
    AVG(employees_clean.salary) AS avg_salary;

DUMP avg_salary_by_dept;

-- ========================================
-- Question 2: Nombre d'employés par département
-- ========================================
emp_count_by_dept = FOREACH emp_by_dept GENERATE
    group AS dept_id,
    COUNT(employees_clean) AS emp_count;

DUMP emp_count_by_dept;

-- ========================================
-- Question 3: Lister tous les employés avec leurs départements
-- ========================================
emp_with_dept = JOIN employees_clean BY dept_id, departments_clean BY dept_id;
emp_dept_list = FOREACH emp_with_dept GENERATE
    employees_clean::emp_id AS emp_id,
    employees_clean::name AS name,
    departments_clean::dept_name AS dept_name,
    employees_clean::salary AS salary;

DUMP emp_dept_list;

-- ========================================
-- Question 4: Employés avec salaire > 60000
-- ========================================
high_salary_emp = FILTER employees_clean BY salary > 60000;
high_salary_result = FOREACH high_salary_emp GENERATE
    emp_id,
    name,
    salary,
    dept_id;

DUMP high_salary_result;

-- ========================================
-- Question 5: Département avec le salaire le plus élevé
-- ========================================
dept_max_salary = FOREACH emp_by_dept GENERATE
    group AS dept_id,
    MAX(employees_clean.salary) AS max_salary;

dept_max_salary_ordered = ORDER dept_max_salary BY max_salary DESC;
top_dept = LIMIT dept_max_salary_ordered 1;

DUMP top_dept;

-- ========================================
-- Question 6: Départements sans employés
-- ========================================
emp_dept_ids = FOREACH employees_clean GENERATE dept_id;
emp_dept_unique = DISTINCT emp_dept_ids;
dept_without_emp = JOIN departments_clean BY dept_id LEFT OUTER, emp_dept_unique BY dept_id;
empty_depts = FILTER dept_without_emp BY emp_dept_unique::dept_id IS NULL;
empty_depts_result = FOREACH empty_depts GENERATE
    departments_clean::dept_id AS dept_id,
    departments_clean::dept_name AS dept_name;

DUMP empty_depts_result;

-- ========================================
-- Question 7: Nombre total d'employés
-- ========================================
all_emp = GROUP employees_clean ALL;
total_emp_count = FOREACH all_emp GENERATE COUNT(employees_clean) AS total;

DUMP total_emp_count;

-- ========================================
-- Question 8: Employés de Paris
-- ========================================
paris_emp = FILTER employees_clean BY city == 'Paris';
paris_emp_result = FOREACH paris_emp GENERATE
    emp_id,
    name,
    city,
    salary;

DUMP paris_emp_result;

-- ========================================
-- Question 9: Salaire total par ville
-- ========================================
emp_by_city = GROUP employees_clean BY city;
total_salary_by_city = FOREACH emp_by_city GENERATE
    group AS city,
    SUM(employees_clean.salary) AS total_salary;

DUMP total_salary_by_city;

-- ========================================
-- Question 10: Départements avec femmes employées
-- ========================================
female_emp = FILTER employees_clean BY gender == 'Female';
female_by_dept = GROUP female_emp BY dept_id;
dept_with_females = FOREACH female_by_dept GENERATE
    group AS dept_id,
    COUNT(female_emp) AS female_count;

-- Joindre avec les noms des départements
dept_with_females_named = JOIN dept_with_females BY dept_id, departments_clean BY dept_id;
dept_females_result = FOREACH dept_with_females_named GENERATE
    departments_clean::dept_id AS dept_id,
    departments_clean::dept_name AS dept_name,
    dept_with_females::female_count AS female_count;

-- Sauvegarder le résultat dans HDFS
STORE dept_females_result INTO '/user/root/pigout/employes_femmes' USING PigStorage(',');

DUMP dept_females_result;
