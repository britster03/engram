// Shortest path between two nodes across structural and semantic edges.
MATCH (s:Node {tenant_id: $tenant_id, source_uri: $src_uri})
MATCH (d:Node {tenant_id: $tenant_id, source_uri: $dst_uri})
MATCH p = shortestPath((s)-[*..4]-(d))
WHERE ALL(n IN nodes(p) WHERE n.tenant_id = $tenant_id AND n.status = 'ACTIVE')
  AND ALL(e IN relationships(p) WHERE coalesce(e.status, 'ACTIVE') = 'ACTIVE')
  AND length(p) <= $max_hops
RETURN [n IN nodes(p) | n.source_uri] AS uri_path,
       [e IN relationships(p) | coalesce(e.relation_label, type(e))] AS edges,
       length(p) AS distance
LIMIT 1
