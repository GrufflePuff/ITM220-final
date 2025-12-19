-- USE games;
-- CREATE VIEW games_view AS 
-- SELECT  Date(`departure`) AS `date`, airlinename, COUNT(1) AS flights_count
-- FROM flight f
-- JOIN airline a
-- USING(airline_id)
-- JOIN airport ap
-- ON ap.airport_id = f.from
-- GROUP BY Date(`departure`), airlinename
SELECT g.game_name, g.release_date, g.price, d.developer_name
FROM games AS g
JOIN developers AS d
ON d.id = g.developers_id;