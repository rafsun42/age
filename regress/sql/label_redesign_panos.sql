--
-- NOTE: TESTS IN THESE FILE WILL BE MOVED TO EXISTING TEST FILES ONCE THEY ARE UPDATED.
--

LOAD 'age'; SET search_path TO ag_catalog;

--
-- Tests if the junction table is created correctly
--

SELECT create_graph('graph');

--
-- Test if the inheritance is correct
--

SELECT * FROM cypher('graph',
$$
		CREATE (u {name: 'name'})
		RETURN u
$$)
AS (u agtype);

SELECT * FROM cypher('graph',
$$
		CREATE (u:label)
		RETURN u
$$)
AS (u agtype);

SELECT * FROM graph._ag_junction_table;

SELECT * FROM graph._ag_label_vertex;

SELECT * FROM graph.label;

SELECT drop_graph('graph', true);






