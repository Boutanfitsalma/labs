-- ============================================
-- Script Pig : Analyse des films (CSV)
-- ============================================

-- Charger les données CSV
films = LOAD '/input/films/films.csv'
  USING PigStorage(',')
  AS (film_id:chararray, title:chararray, year:int, genre:chararray, country:chararray, director_id:chararray);

artists = LOAD '/input/films/artists.csv'
  USING PigStorage(',')
  AS (artist_id:chararray, last_name:chararray, first_name:chararray, birth_date:chararray);

film_actors = LOAD '/input/films/film_actors.csv'
  USING PigStorage(',')
  AS (film_id:chararray, film_title:chararray, actor_id:chararray, role:chararray);

-- Filtrer les lignes d'en-tête
films_clean = FILTER films BY film_id != 'film_id';
artists_clean = FILTER artists BY artist_id != 'artist_id';
film_actors_clean = FILTER film_actors BY film_id != 'film_id';

-- ============================================
-- Q1: Films américains groupés par année
-- ============================================
films_USA = FILTER films_clean BY country == 'US';
mUSA_annee = GROUP films_USA BY year;

mUSA_annee_result = FOREACH mUSA_annee GENERATE
  group AS year,
  COUNT(films_USA) AS nb_films;

DUMP mUSA_annee_result;

-- ============================================
-- Q2: Films américains groupés par réalisateur
-- ============================================
mUSA_director = GROUP films_USA BY director_id;

mUSA_director_result = FOREACH mUSA_director GENERATE
  group AS director_id,
  COUNT(films_USA) AS nb_films;

DUMP mUSA_director_result;

-- ============================================
-- Q3: Triplets (film, acteur, rôle) - Déjà aplati dans film_actors
-- ============================================
mUSA_acteurs = FILTER film_actors_clean BY film_id MATCHES 'movie:.*';

DUMP mUSA_acteurs;

-- ============================================
-- Q4: Joindre films et acteurs avec infos complètes
-- ============================================
moviesActors = JOIN mUSA_acteurs BY actor_id, artists_clean BY artist_id;

moviesActors_clean = FOREACH moviesActors GENERATE
  mUSA_acteurs::film_id AS film_id,
  mUSA_acteurs::film_title AS film_title,
  artists_clean::artist_id AS actor_id,
  artists_clean::first_name AS actor_first_name,
  artists_clean::last_name AS actor_last_name,
  mUSA_acteurs::role AS role;

-- Limiter pour l'affichage
moviesActors_sample = LIMIT moviesActors_clean 10;
DUMP moviesActors_sample;

-- ============================================
-- Q5: Films complets avec tous leurs acteurs (COGROUP)
-- ============================================
fullMovies = COGROUP films_USA BY film_id, moviesActors_clean BY film_id;

fullMovies_formatted = FOREACH fullMovies GENERATE
  group AS film_id,
  FLATTEN(films_USA.title) AS title,
  FLATTEN(films_USA.year) AS year,
  COUNT(moviesActors_clean) AS nb_actors;

-- Limiter pour l'affichage
fullMovies_sample = LIMIT fullMovies_formatted 10;
DUMP fullMovies_sample;

-- ============================================
-- Q6: Acteurs/Réalisateurs - Films où ils ont joué ET réalisé
-- ============================================

-- Films joués par chaque acteur
plays_group = GROUP moviesActors_clean BY actor_id;
plays = FOREACH plays_group GENERATE
  group AS artist_id,
  COUNT(moviesActors_clean) AS nb_films_played;

-- Films réalisés par chaque réalisateur
directs_group = GROUP films_USA BY director_id;
directs = FOREACH directs_group GENERATE
  group AS artist_id,
  COUNT(films_USA) AS nb_films_directed;

-- Joindre avec les artistes
artists_with_plays = JOIN artists_clean BY artist_id LEFT, plays BY artist_id;
artists_full = JOIN artists_with_plays BY artists_clean::artist_id LEFT, directs BY artist_id;

ActeursRealisateurs = FOREACH artists_full GENERATE
  artists_clean::artist_id AS artist_id,
  artists_clean::first_name AS first_name,
  artists_clean::last_name AS last_name,
  (plays::nb_films_played IS NULL ? 0 : plays::nb_films_played) AS nb_films_played,
  (directs::nb_films_directed IS NULL ? 0 : directs::nb_films_directed) AS nb_films_directed;

-- Enregistrer le résultat final
STORE ActeursRealisateurs INTO '/pigout/ActeursRealisateurs'
  USING PigStorage(',');