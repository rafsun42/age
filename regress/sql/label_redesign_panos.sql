--
-- NOTE: TESTS IN THESE FILE WILL BE MOVED TO EXISTING TEST FILES ONCE THEY ARE UPDATED.
--

LOAD 'age'; SET search_path TO ag_catalog;

--
-- Tests if the junction table is created correctly
--

SELECT create_graph('graph');
SELECT * FROM graph._ag_junction_table;

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

--
-- Test that only _ag_label_vertex has _ag_junction_table as parent
--

SELECT * FROM cypher('graph',
$$
		CREATE (u)-[e:REL]->(v)
		RETURN u, v, e
$$)
AS (u agtype, v agtype, e agtype);

SELECT * FROM graph._ag_junction_table;

SELECT * FROM graph._ag_label_edge;

--
-- Test that vertices that are deleted are also correctly deleted from the _ag_junction_table
--

SELECT * FROM cypher('graph',
$$
		MATCH(u {name: 'name'})
		DELETE u
		RETURN u
$$)
AS (u agtype);

SELECT * FROM graph._ag_junction_table;

SELECT * FROM graph._ag_label_vertex;

SELECT drop_graph('graph', true);
