// URI-prefix scoped list; cd-and-ls equivalent for a subtree.
MATCH (n:Node)
WHERE n.tenant_id = $tenant_id
  AND n.source_uri STARTS WITH $prefix
  AND n.status = 'ACTIVE'
RETURN n.source_uri      AS source_uri,
       n.node_type       AS node_type,
       n.l0_abstract     AS l0_abstract,
       n.retrieval_weight AS retrieval_weight
ORDER BY n.source_uri
LIMIT $limit
