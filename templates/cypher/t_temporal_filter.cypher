// Filter RELATES_TO edges of a node by metadata.temporal.valid_from / valid_until.
MATCH (n:Node {tenant_id: $tenant_id, source_uri: $node_uri})-[r:RELATES_TO]->(m:Node)
WHERE m.tenant_id = $tenant_id
  AND r.status = 'ACTIVE'
  AND ($from IS NULL OR coalesce(r.valid_until, '9999-12-31') >= $from)
  AND ($until IS NULL OR coalesce(r.valid_from, '0001-01-01') <= $until)
RETURN m.source_uri     AS source_uri,
       r.relation_label AS relation,
       r.valid_from     AS valid_from,
       r.valid_until    AS valid_until,
       m.l0_abstract    AS l0_abstract
LIMIT $limit
