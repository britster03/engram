"""Filesystem-authoritative storage (§2.2, §5.4.5).

The filesystem is the source of truth; every write is atomic via
temp+rename+fsync. Tenants are isolated by a `tenant_id` sub-directory
under `data_dir`:

    {data_dir}/{tenant_id}/user/entities/alice/alice.md

The public URI scheme stays tenant-relative (`mem://user/entities/alice/...`)
so prompts, manifests, and overviews never need to know their tenant —
that's set by the ambient tenant context resolved from auth.
"""

from __future__ import annotations

import os
from pathlib import Path

from engram import uri as uri_mod
from engram.tenancy import current_tenant_id


class FilesystemStore:
    def __init__(
        self,
        data_dir: str | Path,
        *,
        create_dirs: bool = True,
        tenant_id: str | None = None,
    ) -> None:
        """`tenant_id=None` resolves each call via the ambient context.

        Explicit tenant_id pins the store to one tenant (useful for
        workers). Tests and single-tenant deploys get the `_default`
        tenant when no context is bound.
        """
        self.data_dir = Path(data_dir).resolve()
        self._explicit_tenant = tenant_id
        if create_dirs:
            self.data_dir.mkdir(parents=True, exist_ok=True)

    def _tenant_root(self) -> Path:
        tid = self._explicit_tenant or current_tenant_id()
        root = self.data_dir / tid
        root.mkdir(parents=True, exist_ok=True)
        return root

    def path_for(self, uri: str) -> Path:
        """Resolve a mem:// URI to a filesystem path under the current tenant."""
        return uri_mod.uri_to_path(uri, self._tenant_root())

    def write_atomic(self, uri: str, content: str) -> Path:
        target = self.path_for(uri)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        with open(tmp, "wb") as f:
            f.write(content.encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, target)
        if os.name != 'nt':
            parent_fd = os.open(str(target.parent), os.O_RDONLY)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        return target

    def read(self, uri: str) -> str:
        return self.path_for(uri).read_text(encoding="utf-8")

    def exists(self, uri: str) -> bool:
        return self.path_for(uri).exists()

    def list_children(self, dir_uri: str) -> list[str]:
        """Return child mem:// URIs directly under a directory URI."""
        base = self.path_for(dir_uri)
        if not base.is_dir():
            return []
        out = []
        for child in sorted(base.iterdir()):
            if child.name.startswith("."):
                continue  # hide .manifest from default listings
            child_uri = uri_mod.path_to_uri(child, self._tenant_root())
            out.append(child_uri)
        return out

    def read_manifest(self, dir_uri: str) -> str | None:
        manifest = self.path_for(dir_uri) / ".manifest"
        if not manifest.exists():
            return None
        return manifest.read_text(encoding="utf-8")

    def write_manifest(self, dir_uri: str, content: str) -> None:
        base = self.path_for(dir_uri)
        base.mkdir(parents=True, exist_ok=True)
        manifest = base / ".manifest"
        tmp = manifest.with_suffix(".manifest.tmp")
        with open(tmp, "wb") as f:
            f.write(content.encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, manifest)

    def read_overview(self, dir_uri: str) -> str | None:
        path = self.path_for(dir_uri) / "overview.md"
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8")

    def tenant_scope_path(self) -> Path:
        """Return the absolute path to the current tenant's root (for diagnostics)."""
        return self._tenant_root()
