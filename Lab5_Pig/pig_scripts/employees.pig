-- ============================================
-- Script Pig : Analyse des employés
-- ============================================

-- Charger les données
employees = LOAD '/input/employees/employees.txt'
  USING PigStorage(',')
  AS (id:int, nom:chararray, prenom:chararray, depno:int, ville:chararray, salaire:double);

departments = LOAD '/input/employees/departments.txt'
  USING PigStorage(',')
  AS (depno:int, depname:chararray);

-- ============================================
-- Q1: Salaire moyen par département
-- ============================================
emp_by_dep = GROUP employees BY depno;
avg_salary_by_dep = FOREACH emp_by_dep
  GENERATE group AS depno, AVG(employees.salaire) AS avg_salary;

DUMP avg_salary_by_dep;

-- ============================================
-- Q2: Nombre d'employés par département
-- ============================================
count_emp_by_dep = FOREACH emp_by_dep
  GENERATE group AS depno, COUNT(employees) AS nb_emp;

DUMP count_emp_by_dep;

-- ============================================
-- Q3: Tous les employés avec leurs départements
-- ============================================
emp_with_dep = JOIN employees BY depno, departments BY depno;
emp_with_dep_clean = FOREACH emp_with_dep
  GENERATE employees::id AS id,
           employees::nom AS nom,
           employees::prenom AS prenom,
           departments::depname AS departement,
           employees::ville AS ville,
           employees::salaire AS salaire;

DUMP emp_with_dep_clean;

-- ============================================
-- Q4: Employés avec salaire > 60000
-- ============================================
emp_gt_60000 = FILTER employees BY salaire > 60000;

DUMP emp_gt_60000;

-- ============================================
-- Q5: Département avec le salaire moyen le plus élevé
-- ============================================
top_dep_salary = ORDER avg_salary_by_dep BY avg_salary DESC;
top_dep_salary_one = LIMIT top_dep_salary 1;

-- Joindre avec departments pour avoir le nom
top_dep_with_name = JOIN top_dep_salary_one BY depno, departments BY depno;
top_dep_final = FOREACH top_dep_with_name
  GENERATE departments::depname AS departement,
           top_dep_salary_one::avg_salary AS salaire_moyen;

DUMP top_dep_final;

-- ============================================
-- Q6: Départements sans employés
-- ============================================
dep_emp_join = JOIN departments BY depno LEFT, employees BY depno;
dep_no_emp = FILTER dep_emp_join BY employees::id IS NULL;
dep_no_emp_clean = FOREACH dep_no_emp
  GENERATE departments::depno AS depno,
           departments::depname AS depname;

DUMP dep_no_emp_clean;

-- ============================================
-- Q7: Nombre total d'employés
-- ============================================
all_emp_group = GROUP employees ALL;
total_emp = FOREACH all_emp_group
  GENERATE COUNT(employees) AS total_employees;

DUMP total_emp;

-- ============================================
-- Q8: Employés de Paris
-- ============================================
emp_paris = FILTER employees BY ville == 'Paris';

DUMP emp_paris;

-- ============================================
-- Q9: Salaire total par ville
-- ============================================
emp_by_ville = GROUP employees BY ville;
sum_salary_by_ville = FOREACH emp_by_ville
  GENERATE group AS ville, SUM(employees.salaire) AS total_salary;

DUMP sum_salary_by_ville;

-- ============================================
-- Q10: Départements avec des femmes employées
-- (Utilisation d'une heuristique sur les prénoms)
-- ============================================

-- Définir les prénoms féminins identifiés dans le fichier
femmes = FILTER employees BY 
  (prenom == 'Sophie' OR prenom == 'Marie' OR prenom == 'Claire' OR 
   prenom == 'Anne' OR prenom == 'Julie' OR prenom == 'Emma' OR 
   prenom == 'Chloé' OR prenom == 'Léa' OR prenom == 'Camille' OR 
   prenom == 'Manon');

-- Obtenir les départements distincts avec des femmes
dep_femmes = FOREACH femmes GENERATE depno;
dep_femmes_distinct = DISTINCT dep_femmes;

-- Joindre avec departments pour avoir les noms
dep_femmes_with_name = JOIN dep_femmes_distinct BY depno, departments BY depno;
dep_femmes_final = FOREACH dep_femmes_with_name
  GENERATE departments::depno AS depno,
           departments::depname AS departement;

-- Enregistrer le résultat sur HDFS
STORE dep_femmes_final INTO '/pigout/employes_femmes'
  USING PigStorage(',');