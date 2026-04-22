// ls equivalent: list ACTIVE children by CONTAINS from a directory URI.
MATCH (parent:Node {tenant_id: $tenant_id, source_uri: $uri})-[:CONTAINS]->(child:Node)
WHERE child.tenant_id = $tenant_id AND child.status = 'ACTIVE'
RETURN child.source_uri      AS source_uri,
       child.l0_abstract     AS l0_abstract,
       child.node_type       AS node_type,
       child.retrieval_weight AS retrieval_weight
ORDER BY child.source_uri
LIMIT $limit
