# Raw Kubernetes manifests

These manifests are a staging/reference bundle. They require externally
provisioned PostgreSQL and Temporal, and they do not provide ordered database
migration hooks. Do not use `kubectl apply -k deploy/k8s` as the production
release mechanism.

Production Kubernetes releases must use the Helm chart in
`deploy/helm/engram`, whose pre-install/pre-upgrade hook applies PostgreSQL
migrations before API, worker, and dispatcher rollouts. Alternate local
database backends are not supported by either Kubernetes configuration.
