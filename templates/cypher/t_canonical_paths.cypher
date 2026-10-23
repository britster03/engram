// t_canonical_paths: bounded paths. Every node AND edge must be in scope.
MATCH (start:Node)
WHERE start.tenant_id = $tenant_id AND start.memory_id IN $memory_ids
MATCH p = (start)-[:RELATES_TO*1..4]-(target:Node)
WHERE length(p) <= $hops
  AND ALL(n IN nodes(p) WHERE n.tenant_id = $tenant_id AND n.status IN $statuses)
  AND ALL(r IN relationships(p) WHERE r.tenant_id = $tenant_id
      AND r.status IN $statuses AND r.claim_id IS NOT NULL)
WITH p
ORDER BY length(p), [r IN relationships(p) | r.claim_id]
LIMIT $limit
RETURN [r IN relationships(p) | {
  claim_id: r.claim_id,
  memory_id: startNode(r).memory_id,
  projected_revision: r.projected_revision
}] AS claims
