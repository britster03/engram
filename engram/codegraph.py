"""Deterministic source-code analysis for the Code map.

The normal ingestion pipeline turns prose into semantic memories.  This module
is intentionally separate: it turns a source archive into a *structural* graph
that can be trusted for navigation.  Python uses the standard-library AST;
TypeScript and JavaScript use Tree-sitter when the optional parser packages are
installed.
"""

from __future__ import annotations

import ast
import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any

from slugify import slugify

CODE_SUFFIXES = {".py", ".ts", ".tsx", ".js", ".jsx"}
TEXT_SUFFIXES = {".md", ".txt"}
SUPPORTED_ZIP_SUFFIXES = CODE_SUFFIXES | TEXT_SUFFIXES
EMBEDDED_NODE_TYPES = {"PROJECT", "FILE", "CLASS", "FUNCTION", "METHOD"}
HIERARCHY_EDGE_TYPES = {"CONTAINS", "DEFINES"}


@dataclass(frozen=True)
class CodeSourceFile:
    path: str
    content: str


@dataclass
class CodeNode:
    source_uri: str
    node_type: str
    display_name: str
    parent_uri: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)

    def as_storage_dict(self) -> dict[str, Any]:
        return {
            "source_uri": self.source_uri,
            "properties": {
                "node_type": self.node_type,
                "display_name": self.display_name,
                "status": "ACTIVE",
                "retrieval_weight": 1.0,
                **self.properties,
            },
            "parent_uri": self.parent_uri,
        }


@dataclass
class CodeEdge:
    subject_uri: str
    object_uri: str
    edge_type: str
    relation_label: str
    properties: dict[str, Any] = field(default_factory=dict)

    def as_storage_dict(self) -> dict[str, Any]:
        return {
            "subject_uri": self.subject_uri,
            "object_uri": self.object_uri,
            "edge_type": self.edge_type,
            "relation_label": self.relation_label,
            "properties": {"status": "ACTIVE", **self.properties},
        }


@dataclass
class CodeFileResult:
    path: str
    status: str
    node_count: int = 0
    relationship_count: int = 0
    error_message: str | None = None


@dataclass
class CodeProjectGraph:
    project_name: str
    project_uri: str
    nodes: list[CodeNode] = field(default_factory=list)
    edges: list[CodeEdge] = field(default_factory=list)
    file_results: list[CodeFileResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(item.status != "FAILED" for item in self.file_results)

    def storage_nodes(self) -> list[dict[str, Any]]:
        return [node.as_storage_dict() for node in self.nodes]

    def storage_edges(self) -> list[dict[str, Any]]:
        return [edge.as_storage_dict() for edge in self.edges]


@dataclass
class _PendingRelation:
    subject_uri: str
    target_name: str
    edge_type: str
    relation_label: str
    source_path: str
    line_start: int | None
    line_end: int | None
    preferred_uri: str | None = None


class _Builder:
    def __init__(self, project_name: str) -> None:
        safe_name = slugify(project_name, separator="-")
        if not safe_name:
            raise ValueError("project name must contain letters or numbers")
        self.project_name = project_name.strip()
        self.project_uri = f"mem://projects/{safe_name}"
        self.graph = CodeProjectGraph(project_name=self.project_name, project_uri=self.project_uri)
        self.nodes_by_uri: dict[str, CodeNode] = {}
        self.file_uris: dict[str, str] = {}
        self.symbols: dict[str, list[str]] = {}
        self.pending: list[_PendingRelation] = []
        self._external_by_name: dict[str, str] = {}
        self._add_node(
            CodeNode(
                source_uri=self.project_uri,
                node_type="PROJECT",
                display_name=self.project_name,
                properties={
                    "project_uri": self.project_uri,
                    "l0_abstract": f"Code project {self.project_name}.",
                    "analysis_source": "AST + AI summary",
                },
            )
        )

    def _add_node(self, node: CodeNode) -> None:
        existing = self.nodes_by_uri.get(node.source_uri)
        if existing is None:
            self.nodes_by_uri[node.source_uri] = node
            self.graph.nodes.append(node)

    def add_edge(self, edge: CodeEdge) -> None:
        key = (edge.subject_uri, edge.object_uri, edge.edge_type, edge.relation_label)
        if not any(
            (item.subject_uri, item.object_uri, item.edge_type, item.relation_label) == key
            for item in self.graph.edges
        ):
            self.graph.edges.append(edge)

    def add_directory_chain(self, path: str) -> str:
        parent = self.project_uri
        parts = PurePosixPath(path).parts[:-1]
        for index, part in enumerate(parts, start=1):
            current_path = "/".join(parts[:index])
            uri = f"{self.project_uri}/directories/{current_path}"
            self._add_node(
                CodeNode(
                    source_uri=uri,
                    node_type="DIRECTORY",
                    display_name=part,
                    parent_uri=parent,
                    properties={
                        "project_uri": self.project_uri,
                        "relative_path": current_path,
                        "l0_abstract": f"Folder {current_path} in {self.project_name}.",
                    },
                )
            )
            parent = uri
        return parent

    def add_file(self, source: CodeSourceFile) -> tuple[str, CodeNode]:
        path = source.path.replace("\\", "/").strip("/")
        parent = self.add_directory_chain(path)
        uri = f"{self.project_uri}/files/{path}"
        suffix = PurePosixPath(path).suffix.lower()
        language = _language_for_suffix(suffix)
        node = CodeNode(
            source_uri=uri,
            node_type="FILE",
            display_name=PurePosixPath(path).name,
            parent_uri=parent,
            properties={
                "project_uri": self.project_uri,
                "relative_path": path,
                "language": language,
                "line_start": 1,
                "line_end": max(1, source.content.count("\n") + 1),
                "l0_abstract": f"{language} source file {path}.",
            },
        )
        self._add_node(node)
        self.file_uris[path] = uri
        return uri, node

    def add_symbol(
        self,
        *,
        file_uri: str,
        parent_uri: str | None,
        name: str,
        node_type: str,
        path: str,
        line_start: int | None,
        line_end: int | None,
        signature: str | None = None,
    ) -> str:
        line = line_start or 0
        safe_name = slugify(name, separator="-") or "anonymous"
        uri = f"{file_uri}/symbols/{node_type.lower()}/{safe_name}-{line}"
        owner = parent_uri or file_uri
        label = "method" if node_type == "METHOD" else node_type.lower()
        description = f"{label.title()} {name} defined in {path}."
        self._add_node(
            CodeNode(
                source_uri=uri,
                node_type=node_type,
                display_name=name,
                parent_uri=None,
                properties={
                    "project_uri": self.project_uri,
                    "relative_path": path,
                    "language": _language_for_suffix(PurePosixPath(path).suffix.lower()),
                    "line_start": line_start,
                    "line_end": line_end,
                    "signature": signature,
                    "l0_abstract": description,
                },
            )
        )
        self.add_edge(
            CodeEdge(
                subject_uri=owner,
                object_uri=uri,
                edge_type="DEFINES",
                relation_label="defines",
                properties={
                    "source_path": path,
                    "line_start": line_start,
                    "line_end": line_end,
                    "parser": "ast",
                    "confidence": 1.0,
                    "resolution": "RESOLVED",
                },
            )
        )
        self.symbols.setdefault(name, []).append(uri)
        return uri

    def add_pending(
        self,
        subject_uri: str,
        target_name: str,
        edge_type: str,
        source_path: str,
        line_start: int | None,
        line_end: int | None,
        *,
        preferred_uri: str | None = None,
    ) -> None:
        self.pending.append(
            _PendingRelation(
                subject_uri=subject_uri,
                target_name=target_name,
                edge_type=edge_type,
                relation_label=edge_type.lower().replace("_", " "),
                source_path=source_path,
                line_start=line_start,
                line_end=line_end,
                preferred_uri=preferred_uri,
            )
        )

    def resolve_pending(self) -> None:
        for relation in self.pending:
            candidates = self.symbols.get(relation.target_name, [])
            if relation.preferred_uri and relation.preferred_uri in self.nodes_by_uri:
                target_uri, resolution = relation.preferred_uri, "RESOLVED"
            elif len(candidates) == 1:
                target_uri, resolution = candidates[0], "RESOLVED"
            else:
                target_uri = self._external_node(relation.target_name, relation.edge_type)
                resolution = "EXTERNAL" if relation.target_name else "UNRESOLVED"
            self.add_edge(
                CodeEdge(
                    subject_uri=relation.subject_uri,
                    object_uri=target_uri,
                    edge_type=relation.edge_type,
                    relation_label=relation.relation_label,
                    properties={
                        "source_path": relation.source_path,
                        "line_start": relation.line_start,
                        "line_end": relation.line_end,
                        "parser": "ast",
                        "confidence": 1.0 if resolution == "RESOLVED" else 0.65,
                        "resolution": resolution,
                    },
                )
            )

    def _external_node(self, name: str, relationship: str) -> str:
        label = name or "dynamic expression"
        key = f"{relationship}:{label}"
        existing = self._external_by_name.get(key)
        if existing:
            return existing
        digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
        uri = f"{self.project_uri}/external/{digest}"
        self._external_by_name[key] = uri
        self._add_node(
            CodeNode(
                source_uri=uri,
                node_type="EXTERNAL_MODULE",
                display_name=label,
                properties={
                    "project_uri": self.project_uri,
                    "l0_abstract": f"External or unresolved reference: {label}.",
                    "reference_kind": relationship,
                },
            )
        )
        return uri


def analyze_project(
    project_name: str,
    files: Iterable[CodeSourceFile],
    *,
    core: Any | None = None,
) -> CodeProjectGraph:
    """Analyse a complete source archive without mutating storage.

    A malformed source file makes the project invalid.  The caller must not
    replace a prior graph unless ``graph.ok`` is true.
    """
    builder = _Builder(project_name)
    source_files = sorted(files, key=lambda item: item.path)
    code_files = [
        item for item in source_files if PurePosixPath(item.path).suffix.lower() in CODE_SUFFIXES
    ]
    normalized_files = [
        CodeSourceFile(path=_safe_archive_path(source.path), content=source.content)
        for source in code_files
    ]
    # Register every file before parsing.  Import resolution can then find a
    # module even when the imported file appears later in the ZIP archive.
    file_uris = {source.path: builder.add_file(source)[0] for source in normalized_files}
    for source in normalized_files:
        path = source.path
        file_uri = file_uris[path]
        before_nodes, before_edges = len(builder.graph.nodes), len(builder.graph.edges)
        try:
            suffix = PurePosixPath(path).suffix.lower()
            if suffix == ".py":
                _parse_python(builder, file_uri, path, source.content)
            else:
                _parse_tree_sitter(builder, file_uri, path, source.content, suffix)
            builder.graph.file_results.append(
                CodeFileResult(
                    path=path,
                    status="PARSED",
                    node_count=len(builder.graph.nodes) - before_nodes + 1,
                    relationship_count=len(builder.graph.edges) - before_edges,
                )
            )
        except (SyntaxError, ValueError, RuntimeError) as err:
            builder.graph.file_results.append(
                CodeFileResult(path=path, status="FAILED", error_message=str(err))
            )

    if builder.graph.ok:
        builder.resolve_pending()
        _refresh_file_counts(builder.graph)
        _summarize_project(builder.graph, core)
    return builder.graph


def attach_embeddings(graph: CodeProjectGraph, embed: Any) -> None:
    """Attach existing-model vectors to searchable code stages only."""
    candidates = [node for node in graph.nodes if node.node_type in EMBEDDED_NODE_TYPES]
    if not candidates:
        return
    texts = [
        f"{node.node_type}: {node.display_name}\n{node.properties.get('l0_abstract', '')}"
        for node in candidates
    ]
    vectors = embed.embed_batch(texts)
    for node, vector in zip(candidates, vectors, strict=True):
        node.properties["l0_embedding"] = vector


def _parse_python(builder: _Builder, file_uri: str, path: str, content: str) -> None:
    tree = ast.parse(content, filename=path)

    def visit(
        node: ast.AST, current_symbol: str | None = None, current_class: str | None = None
    ) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node_type = "METHOD" if current_class else "FUNCTION"
            symbol = builder.add_symbol(
                file_uri=file_uri,
                parent_uri=current_class,
                name=node.name,
                node_type=node_type,
                path=path,
                line_start=node.lineno,
                line_end=getattr(node, "end_lineno", node.lineno),
                signature=_python_signature(node),
            )
            for child in node.body:
                visit(child, symbol, current_class)
            return
        if isinstance(node, ast.ClassDef):
            symbol = builder.add_symbol(
                file_uri=file_uri,
                parent_uri=None,
                name=node.name,
                node_type="CLASS",
                path=path,
                line_start=node.lineno,
                line_end=getattr(node, "end_lineno", node.lineno),
                signature=f"class {node.name}",
            )
            for base in node.bases:
                builder.add_pending(
                    symbol,
                    _python_expr_name(base),
                    "EXTENDS",
                    path,
                    node.lineno,
                    getattr(node, "end_lineno", node.lineno),
                )
            for child in node.body:
                visit(child, symbol, symbol)
            return
        if isinstance(node, ast.Import):
            for alias in node.names:
                target = _resolve_python_import(alias.name, path, builder.file_uris)
                builder.add_pending(
                    file_uri,
                    alias.name,
                    "IMPORTS",
                    path,
                    node.lineno,
                    getattr(node, "end_lineno", node.lineno),
                    preferred_uri=target,
                )
            return
        if isinstance(node, ast.ImportFrom):
            module = ("." * node.level) + (node.module or "")
            for alias in node.names:
                reference = f"{module}.{alias.name}".strip(".") or alias.name
                target = _resolve_python_import(module, path, builder.file_uris)
                builder.add_pending(
                    file_uri,
                    reference,
                    "IMPORTS",
                    path,
                    node.lineno,
                    getattr(node, "end_lineno", node.lineno),
                    preferred_uri=target,
                )
            return
        if isinstance(node, ast.Call) and current_symbol:
            builder.add_pending(
                current_symbol,
                _python_expr_name(node.func),
                "CALLS",
                path,
                node.lineno,
                getattr(node, "end_lineno", node.lineno),
            )
        for child_node in ast.iter_child_nodes(node):
            visit(child_node, current_symbol, current_class)

    visit(tree)


def _parse_tree_sitter(
    builder: _Builder,
    file_uri: str,
    path: str,
    content: str,
    suffix: str,
) -> None:
    parser = _tree_sitter_parser(suffix)
    raw = content.encode("utf-8")
    root = parser.parse(raw).root_node
    if root.has_error:
        raise SyntaxError("Tree-sitter could not parse this source file")

    def text(node: Any | None) -> str:
        return (node.text.decode("utf-8") if node is not None else "").strip()

    def visit(
        node: Any, current_symbol: str | None = None, current_class: str | None = None
    ) -> None:
        kind = node.type
        if kind == "import_statement":
            match = re.search(r"(?:from\s+)?[\"']([^\"']+)[\"']", text(node))
            if match:
                reference = match.group(1)
                target = _resolve_js_import(reference, path, builder.file_uris)
                builder.add_pending(
                    file_uri,
                    reference,
                    "IMPORTS",
                    path,
                    node.start_point[0] + 1,
                    node.end_point[0] + 1,
                    preferred_uri=target,
                )
            return
        if kind == "class_declaration":
            name = text(node.child_by_field_name("name")) or "AnonymousClass"
            symbol = builder.add_symbol(
                file_uri=file_uri,
                parent_uri=None,
                name=name,
                node_type="CLASS",
                path=path,
                line_start=node.start_point[0] + 1,
                line_end=node.end_point[0] + 1,
                signature=f"class {name}",
            )
            superclass = text(node.child_by_field_name("superclass"))
            if not superclass:
                heritage = next(
                    (child for child in node.children if child.type == "class_heritage"), None
                )
                match = re.search(r"\bextends\s+([\w.$]+)", text(heritage))
                superclass = match.group(1) if match else ""
            if superclass:
                builder.add_pending(
                    symbol,
                    superclass,
                    "EXTENDS",
                    path,
                    node.start_point[0] + 1,
                    node.end_point[0] + 1,
                )
            for child in node.children:
                visit(child, symbol, symbol)
            return
        if kind in {"function_declaration", "generator_function_declaration"}:
            name = text(node.child_by_field_name("name")) or "anonymous"
            symbol = builder.add_symbol(
                file_uri=file_uri,
                parent_uri=current_class,
                name=name,
                node_type="METHOD" if current_class else "FUNCTION",
                path=path,
                line_start=node.start_point[0] + 1,
                line_end=node.end_point[0] + 1,
                signature=_ts_signature(name, text(node.child_by_field_name("parameters"))),
            )
            for child in node.children:
                visit(child, symbol, current_class)
            return
        if kind == "method_definition":
            name = text(node.child_by_field_name("name")) or "anonymous"
            symbol = builder.add_symbol(
                file_uri=file_uri,
                parent_uri=current_class,
                name=name,
                node_type="METHOD",
                path=path,
                line_start=node.start_point[0] + 1,
                line_end=node.end_point[0] + 1,
                signature=_ts_signature(name, text(node.child_by_field_name("parameters"))),
            )
            for child in node.children:
                visit(child, symbol, current_class)
            return
        if kind == "variable_declarator":
            value = node.child_by_field_name("value")
            if value is not None and value.type in {"arrow_function", "function_expression"}:
                name = text(node.child_by_field_name("name")) or "anonymous"
                symbol = builder.add_symbol(
                    file_uri=file_uri,
                    parent_uri=current_class,
                    name=name,
                    node_type="METHOD" if current_class else "FUNCTION",
                    path=path,
                    line_start=node.start_point[0] + 1,
                    line_end=node.end_point[0] + 1,
                    signature=_ts_signature(name, text(value.child_by_field_name("parameters"))),
                )
                for child in value.children:
                    visit(child, symbol, current_class)
                return
        if kind == "call_expression" and current_symbol:
            func = node.child_by_field_name("function")
            reference = text(func)
            if "." in reference:
                reference = reference.rsplit(".", 1)[-1]
            builder.add_pending(
                current_symbol,
                reference,
                "CALLS",
                path,
                node.start_point[0] + 1,
                node.end_point[0] + 1,
            )
        for child in node.children:
            visit(child, current_symbol, current_class)

    visit(root)


def _tree_sitter_parser(suffix: str) -> Any:
    try:
        import tree_sitter_javascript as javascript
        import tree_sitter_typescript as typescript
        from tree_sitter import Language, Parser
    except ImportError as err:  # pragma: no cover - exercised in deployments without extras
        raise RuntimeError(
            "TypeScript/JavaScript analysis needs tree-sitter parser packages; run pip install -e ."
        ) from err
    capsule = (
        typescript.language_tsx()
        if suffix == ".tsx"
        else typescript.language_typescript()
        if suffix == ".ts"
        else javascript.language()
    )
    language = Language(capsule)
    try:
        return Parser(language)
    except TypeError:  # Tree-sitter < 0.22 compatibility
        parser = Parser()
        legacy_parser: Any = parser
        legacy_parser.set_language(language)
        return parser


def _refresh_file_counts(graph: CodeProjectGraph) -> None:
    for result in graph.file_results:
        file_uri = f"{graph.project_uri}/files/{result.path}"
        descendants = [
            node
            for node in graph.nodes
            if node.source_uri == file_uri or node.source_uri.startswith(file_uri + "/")
        ]
        related = [
            edge
            for edge in graph.edges
            if edge.subject_uri == file_uri or edge.subject_uri.startswith(file_uri + "/")
        ]
        result.node_count = len(descendants)
        result.relationship_count = len(related)
    project = next(node for node in graph.nodes if node.node_type == "PROJECT")
    project.properties["l0_abstract"] = (
        f"Code project {graph.project_name} with {len(graph.file_results)} source files, "
        f"{sum(item.node_count for item in graph.file_results)} code stages, and "
        f"{len(graph.edges)} relationships."
    )


def _summarize_project(graph: CodeProjectGraph, core: Any | None) -> None:
    """Use the configured Core model opportunistically; never block AST facts."""
    if core is None:
        return
    project = next(node for node in graph.nodes if node.node_type == "PROJECT")
    try:
        result = core.complete(
            system_prompt=(
                '[CODE_SUMMARY] Return JSON {"summary": string}. Give a concise, '
                "factual code-project purpose. Do not invent dependencies or behaviour."
            ),
            user_prompt=(
                f"Project: {graph.project_name}\nFiles: "
                + ", ".join(item.path for item in graph.file_results[:40])
            ),
            output_schema={"type": "object", "properties": {"summary": {"type": "string"}}},
            max_tokens=100,
            temperature=0.0,
        )
        output = result.output if isinstance(result.output, dict) else {}
        summary = str(output.get("summary") or "").strip()
        if summary:
            project.properties["l0_abstract"] = summary
    except Exception:
        # The static parser is the authoritative source. AI enrichment is best effort.
        return


def _safe_archive_path(path: str) -> str:
    candidate = PurePosixPath(path.replace("\\", "/"))
    if candidate.is_absolute() or ".." in candidate.parts or not candidate.parts:
        raise ValueError("archive path is unsafe")
    return candidate.as_posix()


def _language_for_suffix(suffix: str) -> str:
    return {
        ".py": "Python",
        ".ts": "TypeScript",
        ".tsx": "TypeScript",
        ".js": "JavaScript",
        ".jsx": "JavaScript",
        ".md": "Markdown",
        ".txt": "Text",
    }.get(suffix, "Text")


def _python_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    args = [arg.arg for arg in node.args.posonlyargs + node.args.args]
    if node.args.vararg:
        args.append("*" + node.args.vararg.arg)
    args.extend(arg.arg for arg in node.args.kwonlyargs)
    if node.args.kwarg:
        args.append("**" + node.args.kwarg.arg)
    prefix = "async def" if isinstance(node, ast.AsyncFunctionDef) else "def"
    return f"{prefix} {node.name}({', '.join(args)})"


def _ts_signature(name: str, parameters: str) -> str:
    return f"{name}{parameters or '()'}"


def _python_expr_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Call):
        return _python_expr_name(node.func)
    try:
        return ast.unparse(node)
    except Exception:
        return "dynamic expression"


def _resolve_python_import(module: str, current_path: str, files: dict[str, str]) -> str | None:
    raw_module = module
    module = raw_module.lstrip(".")
    if not module:
        return None
    relative_level = len(raw_module) - len(module)
    if relative_level:
        base = PurePosixPath(current_path).parent
        for _ in range(max(0, relative_level - 1)):
            base = base.parent
        target = base / module.replace(".", "/")
        candidates = [str(target) + ".py", str(target / "__init__.py")]
    else:
        candidates = [module.replace(".", "/") + ".py", module.replace(".", "/") + "/__init__.py"]
    for candidate in candidates:
        if candidate in files:
            return files[candidate]
    return None


def _resolve_js_import(reference: str, _current_path: str, files: dict[str, str]) -> str | None:
    if not reference.startswith("."):
        return None
    base = str(PurePosixPath(_current_path).parent / reference)
    candidates = [base]
    candidates.extend(base + suffix for suffix in (".ts", ".tsx", ".js", ".jsx"))
    candidates.extend(
        str(PurePosixPath(base) / f"index{suffix}") for suffix in (".ts", ".tsx", ".js", ".jsx")
    )
    for candidate in candidates:
        normal = str(PurePosixPath(candidate))
        if normal in files:
            return files[normal]
    return None
