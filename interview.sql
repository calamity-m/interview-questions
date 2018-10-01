-- MOVIES: title, release_year, genre, director
-- ACTORS: name, birth_year
-- MOVIE_CAST: movie, movie_release_year, actor, birth_year

-- 1. List the title, release year, genre and director of all 'action'? movies.
SELECT * FROM movies WHERE genre = 'action';

-- 2. List the actors and their birth year for all movies directed by 'Wes Anderson'?.
SELECT DISTINCT actor, birth_year 
FROM movie_cast c 
INNER JOIN movies m 
ON c.movie = m.title
AND
m.director = 'Wes Anderson';

-- 3. List the title, release year, genre and director of all movies staring 'Jeff Goldblum'
-- but not 'Bruce Willis'?.
SELECT title, genre, actors, director
FROM movies m
INNER JOIN
(Select movie, string_agg(actor, ', ') AS actors from movie_cast GROUP BY 1) c
ON m.title = c.movie WHERE actors LIKE '%Jeff%' AND actors NOT LIKE '%Bruce%';