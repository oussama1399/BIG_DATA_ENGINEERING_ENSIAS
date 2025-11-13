-- ========================================
-- Script Pig SIMPLE pour l'analyse des films
-- Version simplifiée pour tests rapides
-- ========================================

-- Charger les données des films
films = LOAD '/user/root/input/films.json' 
    USING JsonLoader('_id:int, title:chararray, genres:chararray');

-- Afficher un échantillon
films_sample = LIMIT films 10;
DUMP films_sample;

-- ========================================
-- ANALYSES PRINCIPALES
-- ========================================

-- 1. Nombre total de films
films_grouped = GROUP films ALL;
total_films = FOREACH films_grouped GENERATE 
    COUNT(films) AS nombre_total_films;
DUMP total_films;

-- 2. Extraire et compter les genres
films_with_genres = FOREACH films GENERATE
    _id,
    title,
    FLATTEN(TOKENIZE(genres, '|')) AS genre;

films_genres_clean = FOREACH films_with_genres GENERATE
    _id,
    TRIM(title) AS title,
    TRIM(genre) AS genre;

films_by_genre = GROUP films_genres_clean BY genre;
genre_count = FOREACH films_by_genre GENERATE
    group AS genre,
    COUNT(films_genres_clean) AS nombre_films;

genre_count_sorted = ORDER genre_count BY nombre_films DESC;
DUMP genre_count_sorted;

-- 3. Extraire les années et compter les films par année
films_with_year = FOREACH films GENERATE
    _id,
    title,
    genres,
    REGEX_EXTRACT(title, '.*\\((\\d{4})\\).*', 1) AS year;

films_valid_year = FILTER films_with_year BY year IS NOT NULL;
films_by_year = GROUP films_valid_year BY year;
year_count = FOREACH films_by_year GENERATE
    group AS annee,
    COUNT(films_valid_year) AS nombre_films;

year_count_sorted = ORDER year_count BY nombre_films DESC;
top_10_years = LIMIT year_count_sorted 10;
DUMP top_10_years;

-- 4. Films de comédie
comedy_films = FILTER films_genres_clean BY genre == 'Comedy';
comedy_count_group = GROUP comedy_films ALL;
comedy_count = FOREACH comedy_count_group GENERATE
    COUNT(comedy_films) AS nombre_comedies;
DUMP comedy_count;
