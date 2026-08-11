// Provenance REFERENCES adjacent to a node. Traversal is deliberately
// bidirectional: episode -> fact -> entity is the storage orientation, while
// retrieval usually starts from an entity or fact and must reach its source.
MATCH (n:Node {tenant_id: $tenant_id, source_uri: $node_uri})-[r:REFERENCES]-(m:Node)
WHERE m.tenant_id = $tenant_id
  AND m.status = 'ACTIVE'
  AND coalesce(r.status, 'ACTIVE') = 'ACTIVE'
RETURN DISTINCT m.source_uri   AS source_uri,
       m.node_type    AS node_type,
       r.relation_label AS relation,
       m.l0_abstract  AS l0_abstract
LIMIT $limit
