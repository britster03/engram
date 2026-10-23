"""Structural source-code graph extraction tests."""

from __future__ import annotations

import pytest

from engram.codegraph import CodeSourceFile, analyze_project
from engram.storage.memory_kg import InMemoryKnowledgeGraph


def _relations(graph):
    return {(edge.edge_type, edge.properties.get("resolution")) for edge in graph.edges}


def test_python_analysis_extracts_definitions_and_relationships():
    graph = analyze_project(
        "Billing service",
        [
            CodeSourceFile(
                "app/service.py",
                "from .repository import find_invoice\n"
                "class BillingService(BaseService):\n"
                "    def charge(self, invoice_id):\n"
                "        return find_invoice(invoice_id)\n",
            ),
            CodeSourceFile(
                "app/repository.py", "def find_invoice(invoice_id):\n    return invoice_id\n"
            ),
        ],
    )

    assert graph.ok
    assert {node.node_type for node in graph.nodes} >= {
        "PROJECT",
        "DIRECTORY",
        "FILE",
        "CLASS",
        "METHOD",
        "FUNCTION",
    }
    assert ("IMPORTS", "RESOLVED") in _relations(graph)
    assert ("CALLS", "RESOLVED") in _relations(graph)
    assert any(
        edge.edge_type == "EXTENDS" and edge.properties["resolution"] == "EXTERNAL"
        for edge in graph.edges
    )
    charge = next(node for node in graph.nodes if node.display_name == "charge")
    assert charge.properties["signature"] == "def charge(self, invoice_id)"
    assert charge.properties["line_start"] == 3


def test_invalid_python_preserves_a_failed_analysis_result():
    graph = analyze_project("Broken project", [CodeSourceFile("broken.py", "def nope(:\n")])
    assert not graph.ok
    assert graph.file_results[0].status == "FAILED"
    assert graph.nodes[0].node_type == "PROJECT"


def test_typescript_analysis_extracts_import_calls_and_inheritance():
    pytest.importorskip("tree_sitter")
    graph = analyze_project(
        "Web API",
        [
            CodeSourceFile(
                "src/api.ts",
                "import { login } from './auth';\n"
                "export class Api extends BaseApi { handle(user: string) { return login(user); } }\n"
                "export const boot = () => login('demo');\n",
            ),
            CodeSourceFile("src/auth.ts", "export function login(user: string) { return user; }\n"),
        ],
    )

    assert graph.ok
    assert {node.display_name for node in graph.nodes} >= {"Api", "handle", "boot", "login"}
    assert ("IMPORTS", "RESOLVED") in _relations(graph)
    assert ("CALLS", "RESOLVED") in _relations(graph)
    assert any(edge.edge_type == "EXTENDS" for edge in graph.edges)


def test_reupload_replaces_only_the_named_project_graph():
    store = InMemoryKnowledgeGraph()
    first = analyze_project(
        "Payments", [CodeSourceFile("old.py", "def old_handler():\n    return 1\n")]
    )
    other = analyze_project(
        "Users", [CodeSourceFile("user.py", "def create_user():\n    return 1\n")]
    )
    store.replace_code_project(
        project_uri=first.project_uri,
        nodes=first.storage_nodes(),
        edges=first.storage_edges(),
        tenant_id="t1",
    )
    store.replace_code_project(
        project_uri=other.project_uri,
        nodes=other.storage_nodes(),
        edges=other.storage_edges(),
        tenant_id="t1",
    )
    refreshed = analyze_project(
        "Payments", [CodeSourceFile("new.py", "def charge():\n    return 1\n")]
    )
    store.replace_code_project(
        project_uri=refreshed.project_uri,
        nodes=refreshed.storage_nodes(),
        edges=refreshed.storage_edges(),
        tenant_id="t1",
    )

    labels = {node.get("display_name") for _uri, node in store.iter_nodes(tenant_id="t1")}
    assert "old_handler" not in labels
    assert {"charge", "create_user"} <= labels
