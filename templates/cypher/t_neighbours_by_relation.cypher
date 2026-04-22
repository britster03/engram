// Follow RELATES_TO edges of a given label up to N hops.
MATCH (n:Node {tenant_id: $tenant_id, source_uri: $node_uri})
CALL {
  WITH n
  MATCH p = (n)-[r:RELATES_TO*1..4]->(m:Node)
  WHERE m.tenant_id = $tenant_id
    AND ALL(edge IN relationships(p) WHERE edge.status = 'ACTIVE')
    AND ALL(edge IN relationships(p) WHERE edge.relation_label = $relation OR $relation IS NULL)
    AND length(p) <= $hops
    AND m.status = 'ACTIVE'
  RETURN m, p
}
RETURN m.source_uri    AS source_uri,
       m.l0_abstract   AS l0_abstract,
       m.node_type     AS node_type,
       length(p)       AS distance
ORDER BY distance, source_uri
LIMIT $limit
