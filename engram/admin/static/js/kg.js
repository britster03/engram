function kgGraph() {
  return {
    rootUri: '',
    nodeType: '',
    statusFilter: '',
    depth: 2,
    limit: 120,
    graph: { nodes: [], edges: [] },
    selected: null,
    summary: '0 nodes, 0 edges',
    error: '',

    async init() {
      await this.load();
      window.addEventListener('resize', () => this.draw());
    },

    async load() {
      this.error = '';
      const params = new URLSearchParams();
      if (this.rootUri.trim()) params.set('root_uri', this.rootUri.trim());
      if (this.nodeType) params.set('type', this.nodeType);
      params.set('depth', String(Math.max(0, Math.min(4, Number(this.depth) || 0))));
      params.set('limit', String(Math.max(1, Math.min(500, Number(this.limit) || 100))));
      try {
        const resp = await fetch('/admin/api/kg/graph?' + params.toString(), {
          headers: this._authHeaders(),
        });
        if (resp.status === 401) {
          window.location.href = '/admin/login';
          return;
        }
        if (!resp.ok) {
          const data = await resp.json().catch(() => ({}));
          throw new Error(data.detail || resp.statusText);
        }
        this.graph = await resp.json();
        this.selected = null;
        this.draw();
      } catch (err) {
        this.error = err.message || String(err);
      }
    },

    draw() {
      const canvas = document.getElementById('kg-canvas');
      if (!canvas || !window.EngramGraph) return;
      const nodes = (this.graph.nodes || []).filter((n) => {
        return !this.statusFilter || n.status === this.statusFilter;
      });
      const ids = new Set(nodes.map((n) => n.id));
      const edges = (this.graph.edges || []).filter((e) => ids.has(e.source) && ids.has(e.target));
      this.summary = `${nodes.length} nodes, ${edges.length} edges`;
      window.EngramGraph.render(canvas, { nodes, edges }, {
        onSelect: (node) => { this.selected = node; },
      });
    },

    openMemory() {
      if (!this.selected || !this.selected.source_uri) return;
      const prefix = encodeURIComponent(this.selected.source_uri);
      window.location.href = '/admin/dashboard?tab=memories&prefix=' + prefix;
    },

    _authHeaders() {
      const headers = {};
      const token = localStorage.getItem('engram_api_key');
      if (token) headers.Authorization = 'Bearer ' + token;
      return headers;
    },
  };
}
