--
-- NOTE: TESTS IN THESE FILE WILL BE MOVED TO EXISTING TEST FILES ONCE THEY ARE UPDATED.
--

LOAD 'age'; SET search_path TO ag_catalog;

--
-- Tests vertex\edge ID, ID sequences label_id column
--
SELECT create_graph('graph1');

SELECT * FROM cypher('graph1',
$$
		CREATE (:Teacher{name:'John'})-[:Teaches{from:2011}]->(:Class{title:'DS'}),
		(:Student{name:'Smith'})-[:Learns{from:2013}]->(:Class{title:'AI'}),
		({name:'Bob'})-[:Learns{from:2022}]->({title:'Swimming'})
$$)
as (a agtype);

-- labels
SELECT name, id, kind, relation, seq_name
FROM ag_catalog.ag_label
WHERE graph IN (
    SELECT graphid
    FROM ag_catalog.ag_graph
    WHERE name = 'graph1'
)
ORDER BY id;
-- vertices
SELECT * FROM graph1._ag_label_vertex ORDER BY id;
-- edges
SELECT * FROM graph1._ag_label_edge ORDER BY id;

-- test vertex, edge and path object
SELECT * FROM cypher('graph1', $$ MATCH (x) RETURN x $$) AS (result agtype);
SELECT * FROM cypher('graph1', $$ MATCH ()-[e]->() RETURN e $$) AS (result agtype);
SELECT * FROM cypher('graph1', $$ MATCH p = ()-[]->() RETURN p $$) AS (result agtype);

-- show vertex and edge relation information
\d+ graph1._ag_label_vertex;
\d+ graph1._ag_label_edge;

SELECT drop_graph('graph1', true);