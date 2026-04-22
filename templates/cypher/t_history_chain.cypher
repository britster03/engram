// Follow SUPERSEDES edges to get the full version history. Returns HISTORICAL nodes on purpose.
MATCH (latest:Node {tenant_id: $tenant_id, source_uri: $node_uri})
MATCH path = (latest)-[:SUPERSEDES*0..]->(n:Node)
WHERE n.tenant_id = $tenant_id
RETURN n.source_uri    AS source_uri,
       n.status        AS status,
       n.l0_abstract   AS l0_abstract,
       n.created_at    AS created_at,
       length(path)    AS distance
ORDER BY distance
LIMIT $limit
