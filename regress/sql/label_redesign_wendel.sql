--
-- NOTE: TESTS IN THESE FILE WILL BE MOVED TO EXISTING TEST FILES ONCE THEY ARE UPDATED.
--

LOAD 'age'; SET search_path TO ag_catalog;

--
-- Tests entity_exists function and get_label_id_from_entity
--
SELECT create_graph('graph1');

SELECT * FROM cypher('graph1', $$ CREATE (:Developer{name:'Brian'}) $$) AS (a agtype);

-- get_label_id_from_entity from set clause
SELECT * FROM cypher('graph1', $$ MATCH (x:Developer) SET x.yearOfBirth = 1982, x.surname = 'Jones' RETURN x $$) AS (vertex agtype);

-- entity_exists returning false
SELECT * FROM cypher('graph1', $$ MATCH (x:Developer) DELETE (x) CREATE p=(x)-[r:PARTICIPATES{from:2023}]->(y:Project{title:'Documentation'}) RETURN p $$) AS (path agtype);

SELECT * FROM cypher('graph1', $$ MATCH (x:Developer) DELETE (x) MERGE p=(x)-[r:PARTICIPATES{from:2023}]->(y:Project{title:'Documentation'}) RETURN p $$) AS (path agtype);

-- entity_exists returning true
SELECT * FROM cypher('graph1', $$ MATCH (x:Developer) CREATE p=(x)-[r:PARTICIPATES{from:2021}]->(y:Project{title:'Dashboard'}) RETURN p $$) AS (path agtype);

SELECT * FROM cypher('graph1', $$ MATCH (x:Developer) MERGE p=(x)-[r:PARTICIPATES{from:2016}]->(y:Project{title:'Login Rework'}) RETURN p $$) AS (path agtype);

SELECT drop_graph('graph1', true);
