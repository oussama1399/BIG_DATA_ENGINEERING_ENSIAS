-- ============================================
-- ANALYSE DES FILMS AVEC APACHE PIG
-- ============================================
-- Script d'analyse du dataset films.json
-- Format: { "_id" : 1, "title" : "Toy Story (1995)", "genres" : "Animation|Children's|Comedy" }

-- ============================================
-- CHARGEMENT DES DONNÉES
-- ============================================

-- Charger les films depuis HDFS
films = LOAD 'input/films.json' 
    USING JsonLoader('_id:int, title:chararray, genres:chararray');

-- Charger les films depuis HDFS
films = LOAD 'input/films.json' 
    USING JsonLoader('_id:int, title:chararray, genres:chararray');

-- Afficher un échantillon pour vérification
films_sample = LIMIT films 5;
DUMP films_sample;


-- ============================================
-- QUESTION 1 : Nombre total de films
-- ============================================
films_grouped = GROUP films ALL;
total_films = FOREACH films_grouped GENERATE 
    COUNT(films) AS nombre_total;

DUMP total_films;
STORE total_films INTO 'pigout/films/total_count' USING PigStorage(',');


-- ============================================
-- QUESTION 2 : Films groupés par année
-- ============================================
-- Extraire l'année du titre (format: "Titre (YYYY)")
films_with_year = FOREACH films GENERATE
    _id,
    title,
    genres,
    REGEX_EXTRACT(title, '.*\\((\\d{4})\\).*', 1) AS year;

-- Filtrer les films avec une année valide
films_valid_year = FILTER films_with_year BY year IS NOT NULL;

-- Grouper par année
films_by_year = GROUP films_valid_year BY year;

-- Compter les films par année
year_count = FOREACH films_by_year GENERATE
    group AS annee,
    COUNT(films_valid_year) AS nb_films,
    films_valid_year.(title, genres) AS films;

-- Trier par année
year_count_sorted = ORDER year_count BY annee;

DUMP year_count_sorted;
STORE year_count_sorted INTO 'pigout/films/films_by_year' USING PigStorage('|');


-- ============================================
-- QUESTION 3 : Aplatir les genres (FLATTEN)
-- ============================================
-- Séparer les genres multiples (séparés par |)
films_with_genres = FOREACH films GENERATE
    _id,
    title,
    FLATTEN(TOKENIZE(genres, '|')) AS genre;

-- Nettoyer les espaces
films_genres_clean = FOREACH films_with_genres GENERATE
    _id,
    TRIM(title) AS title,
    TRIM(genre) AS genre;

-- Afficher un échantillon
genres_sample = LIMIT films_genres_clean 20;
DUMP genres_sample;


-- ============================================
-- QUESTION 4 : Nombre de films par genre
-- ============================================
-- Grouper par genre
films_by_genre = GROUP films_genres_clean BY genre;

-- Compter les films par genre
genre_count = FOREACH films_by_genre GENERATE
    group AS genre,
    COUNT(films_genres_clean) AS nb_films;

-- Trier par nombre de films (décroissant)
genre_count_sorted = ORDER genre_count BY nb_films DESC;

DUMP genre_count_sorted;
STORE genre_count_sorted INTO 'pigout/films/genre_count' USING PigStorage(',');


-- ============================================
-- QUESTION 5 : Top 10 des genres les plus populaires
-- ============================================
top_10_genres = LIMIT genre_count_sorted 10;

DUMP top_10_genres;
STORE top_10_genres INTO 'pigout/films/top_10_genres' USING PigStorage(',');


-- ============================================
-- QUESTION 6 : Top 10 des années avec le plus de films
-- ============================================
year_count_by_popularity = ORDER year_count BY nb_films DESC;
top_10_years = LIMIT year_count_by_popularity 10;

DUMP top_10_years;
STORE top_10_years INTO 'pigout/films/top_10_years' USING PigStorage('|');


-- ============================================
-- QUESTION 7 : Films de comédie (Comedy)
-- ============================================
comedy_films = FILTER films_genres_clean BY genre == 'Comedy';

-- Grouper par film pour supprimer les doublons
comedy_unique = FOREACH (GROUP comedy_films BY (_id, title)) GENERATE
    FLATTEN(group) AS (_id, title);

-- Compter les films de comédie
comedy_count_group = GROUP comedy_unique ALL;
comedy_count = FOREACH comedy_count_group GENERATE
    COUNT(comedy_unique) AS nb_comedies;

DUMP comedy_count;
STORE comedy_count INTO 'pigout/films/comedy_count' USING PigStorage(',');


-- ============================================
-- QUESTION 8 : Films d'action (Action)
-- ============================================
action_films = FILTER films_genres_clean BY genre == 'Action';

-- Grouper par film pour supprimer les doublons
action_unique = FOREACH (GROUP action_films BY (_id, title)) GENERATE
    FLATTEN(group) AS (_id, title);

-- Compter les films d'action
action_count_group = GROUP action_unique ALL;
action_count = FOREACH action_count_group GENERATE
    COUNT(action_unique) AS nb_actions;

DUMP action_count;
STORE action_count INTO 'pigout/films/action_count' USING PigStorage(',');


-- ============================================
-- QUESTION 9 : Films avec plusieurs genres
-- ============================================
-- Grouper par film pour compter ses genres
films_genre_count = FOREACH (GROUP films_genres_clean BY (_id, title)) GENERATE
    FLATTEN(group) AS (_id, title),
    COUNT(films_genres_clean) AS nb_genres;

-- Filtrer les films avec plus d'un genre
multi_genre_films = FILTER films_genre_count BY nb_genres > 1;

-- Compter combien de films ont plusieurs genres
multi_genre_count_group = GROUP multi_genre_films ALL;
multi_genre_count = FOREACH multi_genre_count_group GENERATE
    COUNT(multi_genre_films) AS nb_films_multi_genres;

DUMP multi_genre_count;
STORE multi_genre_count INTO 'pigout/films/multi_genre_count' USING PigStorage(',');


-- ============================================
-- QUESTION 10 : Films d'animation pour enfants
-- ============================================
-- Filtrer les films avec Animation
animation_films = FILTER films BY genres MATCHES '.*Animation.*';

-- Filtrer ceux qui ont aussi Children's
animation_children = FILTER animation_films BY genres MATCHES '.*Children.*';

-- Compter
animation_children_count_group = GROUP animation_children ALL;
animation_children_count = FOREACH animation_children_count_group GENERATE
    COUNT(animation_children) AS nb_animation_children;

DUMP animation_children_count;
STORE animation_children_count INTO 'pigout/films/animation_children_count' USING PigStorage(',');

-- Lister quelques exemples
animation_children_list = FOREACH animation_children GENERATE
    _id,
    title,
    genres;

animation_children_sample = LIMIT animation_children_list 10;
DUMP animation_children_sample;
STORE animation_children_list INTO 'pigout/films/animation_children_list' USING PigStorage(',');


-- ============================================
-- QUESTION 11 : Films de 1995
-- ============================================
films_1995 = FILTER films_valid_year BY year == '1995';

-- Compter les films de 1995
films_1995_count_group = GROUP films_1995 ALL;
films_1995_count = FOREACH films_1995_count_group GENERATE
    COUNT(films_1995) AS nb_films_1995;

DUMP films_1995_count;
STORE films_1995_count INTO 'pigout/films/films_1995_count' USING PigStorage(',');

-- Liste des films de 1995
films_1995_list = FOREACH films_1995 GENERATE
    _id,
    title,
    genres;

DUMP films_1995_list;
STORE films_1995_list INTO 'pigout/films/films_1995_list' USING PigStorage(',');


-- ============================================
-- QUESTION 12 : Top 20 des combinaisons de genres
-- ============================================
-- Grouper par combinaison de genres
films_by_genre_combo = GROUP films BY genres;

-- Compter les films par combinaison
genre_combo_count = FOREACH films_by_genre_combo GENERATE
    group AS combinaison_genres,
    COUNT(films) AS nb_films;

-- Trier par nombre de films
genre_combo_sorted = ORDER genre_combo_count BY nb_films DESC;

-- Top 20 des combinaisons
top_genre_combos = LIMIT genre_combo_sorted 20;

DUMP top_genre_combos;
STORE top_genre_combos INTO 'pigout/films/top_genre_combinations' USING PigStorage(',');


-- ============================================
-- FIN DU SCRIPT
-- ============================================
