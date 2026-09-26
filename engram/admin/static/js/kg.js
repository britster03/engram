function kgGraph() {
  return {
    view: 'semantic', rootUri: '', search: '', sourceSearch: '', nodeType: '', statusFilter: '', depth: 1, limit: 40,
    graph: { nodes: [], edges: [] }, selected: null, selectedDetails: null, selectedLoading: false,
    summary: 'No memories loaded', error: '', loading: false,
    codeProjects: [], codeProjectUri: '', codeMap: { nodes: [], edges: [] }, codeDepth: 2, codeLimit: 200,
    codeSearch: '', codeSelected: null, codeDetails: null, codeLoading: false, collapsedCodeNodes: [],

    async init() {
      const params = new URLSearchParams(window.location.search);
      this.view = params.get('view') === 'code' ? 'code' : 'semantic';
      this.codeProjectUri = params.get('project') || '';
      if (this.view === 'code') await this.loadCodeProjects(); else await this.load();
      window.addEventListener('resize', () => this.draw());
    },

    async setView(view) {
      this.view = view; this.error = '';
      const url = new URL(window.location.href);
      if (view === 'code') { url.searchParams.set('view', 'code'); await this.loadCodeProjects(); }
      else { url.searchParams.delete('view'); url.searchParams.delete('project'); await this.load(); }
      window.history.replaceState({}, '', url);
    },

    async load(selectedUri = null) {
      this.error = ''; this.loading = true;
      const selectionToRestore = selectedUri || (this.selected && this.selected.source_uri);
      const params = new URLSearchParams();
      if (this.rootUri.trim()) params.set('root_uri', this.rootUri.trim());
      if (this.nodeType) params.set('type', this.nodeType);
      params.set('depth', String(Math.max(0, Math.min(4, Number(this.depth) || 0))));
      params.set('limit', String(Math.max(1, Math.min(500, Number(this.limit) || 100))));
      try {
        const resp = await fetch('/admin/api/kg/graph?' + params.toString(), { headers: this._authHeaders() });
        if (resp.status === 401) return this._login();
        if (!resp.ok) throw new Error((await resp.json().catch(() => ({}))).detail || resp.statusText);
        this.graph = await resp.json();
        this.selected = selectionToRestore
          ? (this.graph.nodes || []).find((node) => node.source_uri === selectionToRestore) || null
          : null;
        this.selectedDetails = null;
        this.draw();
        if (this.selected) await this.loadSelectedDetails();
      } catch (err) { this.error = err.message || String(err); } finally { this.loading = false; }
    },

    async loadCodeProjects() {
      this.error = ''; this.codeLoading = true;
      try {
        const response = await fetch('/admin/api/kg/projects', { headers: this._authHeaders() });
        if (response.status === 401) return this._login();
        if (!response.ok) throw new Error((await response.json().catch(() => ({}))).detail || response.statusText);
        this.codeProjects = await response.json();
        if (!this.codeProjectUri && this.codeProjects.length) this.codeProjectUri = this.codeProjects[0].source_uri;
        if (this.codeProjectUri) await this.loadCodeMap();
      } catch (err) { this.error = err.message || String(err); } finally { this.codeLoading = false; }
    },

    async loadCodeMap() {
      if (!this.codeProjectUri) return;
      this.error = ''; this.codeLoading = true;
      const params = new URLSearchParams({ project_uri: this.codeProjectUri, depth: String(this.codeDepth), limit: String(this.codeLimit) });
      try {
        const response = await fetch('/admin/api/kg/code-map?' + params.toString(), { headers: this._authHeaders() });
        if (response.status === 401) return this._login();
        if (!response.ok) throw new Error((await response.json().catch(() => ({}))).detail || response.statusText);
        this.codeMap = await response.json(); this.codeSelected = null; this.codeDetails = null; this.collapsedCodeNodes = [];
        const url = new URL(window.location.href); url.searchParams.set('view', 'code'); url.searchParams.set('project', this.codeProjectUri);
        window.history.replaceState({}, '', url);
      } catch (err) { this.error = err.message || String(err); } finally { this.codeLoading = false; }
    },

    draw() {
      if (this.view !== 'semantic') return;
      const canvas = document.getElementById('kg-canvas'); if (!canvas || !window.EngramGraph) return;
      const nodes = (this.graph.nodes || []).filter((node) => {
        if (this.statusFilter && node.status !== this.statusFilter) return false;
        const term = this.search.trim().toLowerCase();
        return !term || this.displayName(node).toLowerCase().includes(term) || String(node.l0_abstract || '').toLowerCase().includes(term);
      });
      const ids = new Set(nodes.map((node) => node.id));
      const edges = (this.graph.edges || []).filter((edge) => ids.has(edge.source) && ids.has(edge.target));
      const sourceCount = nodes.filter((node) => this.isDocumentLike(node)).length;
      this.summary = `${sourceCount} sources · ${nodes.length} memories · ${edges.length} connections`;
      window.EngramGraph.render(canvas, { nodes, edges }, { selectedId: this.selected && this.selected.id, showSelectedEdgeLabels: true, onSelect: (node) => { this.selectNode(node); } });
    },

    async selectNode(node) {
      if (!node) return;
      const isCurrentSelection = this.selected && this.selected.id === node.id;
      if (isCurrentSelection && (this.selectedLoading || this.selectedDetails)) {
        this.draw();
        return;
      }
      this.selected = node; this.selectedDetails = null; this.draw();
      await this.loadSelectedDetails();
    },

    async loadSelectedDetails() {
      if (!this.selected || !this.selected.source_uri) return;
      this.selectedLoading = true;
      try {
        const params = new URLSearchParams({ source_uri: this.selected.source_uri });
        const response = await fetch('/admin/api/memories/detail?' + params.toString(), { headers: this._authHeaders() });
        if (response.status === 401) return this._login();
        if (!response.ok) throw new Error((await response.json().catch(() => ({}))).detail || response.statusText);
        this.selectedDetails = await response.json();
      } catch (err) {
        this.error = err.message || String(err);
      } finally {
        this.selectedLoading = false;
      }
    },

    codeChildren(uri) { return (this.codeMap.edges || []).filter((edge) => edge.source === uri).map((edge) => edge.target); },
    codeNode(uri) { return (this.codeMap.nodes || []).find((node) => node.id === uri); },
    codeNodeColor(node) {
      if (this.codeSelected && this.codeSelected.id === node.id) return '#bfdbfe';
      if (node.node_type === 'PROJECT' || node.node_type === 'FILE' || node.node_type === 'DIRECTORY') return '#dbeafe';
      if (node.node_type === 'CLASS') return '#ede9fe';
      if (node.node_type === 'METHOD') return '#fef3c7';
      return '#d1fae5';
    },
    codeTreeLayout() {
      const term = this.codeSearch.trim().toLowerCase(); const collapsed = new Set(this.collapsedCodeNodes); const output = []; let row = 0;
      const visit = (uri, level) => {
        const node = this.codeNode(uri); if (!node) return;
        const matches = !term || this.displayName(node).toLowerCase().includes(term) || String(node.l0_abstract || '').toLowerCase().includes(term);
        if (matches) output.push({ ...node, x: 36 + level * 245, y: 42 + row++ * 78, level, hasChildren: this.codeChildren(uri).length > 0 });
        if (!collapsed.has(uri)) this.codeChildren(uri).forEach((child) => visit(child, level + 1));
      };
      visit(this.codeProjectUri, 0); return output;
    },
    codeTreeLinks() {
      const points = new Map(this.codeTreeLayout().map((node) => [node.id, node]));
      return (this.codeMap.edges || []).map((edge) => ({ ...edge, sourceNode: points.get(edge.source), targetNode: points.get(edge.target) })).filter((edge) => edge.sourceNode && edge.targetNode);
    },
    codeViewBox() { const nodes = this.codeTreeLayout(); return `0 0 ${Math.max(900, ...nodes.map((node) => node.x + 225))} ${Math.max(560, ...nodes.map((node) => node.y + 50))}`; },
    codeCanvasHeight() { return Math.max(560, this.codeTreeLayout().length * 78 + 80); },
    toggleCodeNode(node) { if (!node || !node.hasChildren) return; const index = this.collapsedCodeNodes.indexOf(node.id); if (index === -1) this.collapsedCodeNodes.push(node.id); else this.collapsedCodeNodes.splice(index, 1); },
    async selectCodeNode(node) {
      if (!node) return; this.codeSelected = node; this.codeDetails = null;
      try {
        const response = await fetch('/admin/api/kg/node-details?' + new URLSearchParams({ source_uri: node.source_uri }), { headers: this._authHeaders() });
        if (response.status === 401) return this._login();
        if (!response.ok) throw new Error((await response.json().catch(() => ({}))).detail || response.statusText);
        this.codeDetails = await response.json();
      } catch (err) { this.error = err.message || String(err); }
    },
    codeRelations(direction) { return ((this.codeDetails && this.codeDetails[direction]) || []).filter((item) => item.source_uri && !['CONTAINS', 'DEFINES'].includes(item.relation)); },

    displayName(node) {
      if (node && node.label && !String(node.label).startsWith('mem://')) return String(node.label);
      const source = String((node && (node.source_uri || node.label || node.id)) || 'Memory');
      if (node && node.node_type === 'ENTITY' && source.includes('/entities/')) { const parts = source.split('/').filter(Boolean); return this.humanize(parts[parts.length - 2] || parts[parts.length - 1]); }
      if (source.includes('/episodes/')) return this.humanize((source.split('/').pop() || source).replace(/^\d{4}-\d{2}-\d{2}_/, '').replace(/\.md$/, ''));
      return this.humanize((source.split('/').pop() || source).replace(/\.md$/, ''));
    },
    humanize(value) { return String(value || 'Memory').replace(/[-_]+/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase()); },
    typeLabel(node) {
      const type = String((node && node.node_type) || 'MEMORY');
      if (type === 'EPISODE') return 'Conversation';
      if (type === 'SESSION_SUMMARY') return 'Session summary';
      return this.humanize(type.toLowerCase());
    },
    sourceTitle(node) {
      if (node && node.node_type === 'ENTITY') return this.displayName(node);
      const abstract = String((node && node.l0_abstract) || '').trim();
      return abstract || this.displayName(node);
    },
    shortReference(node) {
      const label = this.displayName(node);
      if (!label.startsWith('Conversation ')) return label;
      return label.replace('Conversation ', 'Event ');
    },
    connectionCount(node) {
      if (!node) return 0;
      const uniqueConnections = new Set();
      (this.graph.edges || [])
        .filter((edge) => edge.source === node.id || edge.target === node.id)
        .forEach((edge) => {
          const outgoing = edge.source === node.id;
          const connectedId = outgoing ? edge.target : edge.source;
          uniqueConnections.add(`${outgoing ? 'outgoing' : 'incoming'}|${edge.label || edge.type || 'related to'}|${connectedId}`);
        });
      return uniqueConnections.size;
    },
    selectedBody() {
      return String((this.selectedDetails && this.selectedDetails.body) || (this.selected && this.selected.l0_abstract) || '').trim();
    },
    selectedAbstract() {
      return String(
        (this.selectedDetails && this.selectedDetails.current_version && this.selectedDetails.current_version.abstract)
        || (this.selected && this.selected.l0_abstract)
        || ''
      ).trim();
    },
    selectedClaims() { return (this.selectedDetails && this.selectedDetails.claims) || []; },
    selectedEvidence() { return (this.selectedDetails && this.selectedDetails.evidence) || []; },
    claimObject(claim) {
      if (!claim) return 'Unknown';
      if (claim.object_value !== null && claim.object_value !== undefined) {
        return typeof claim.object_value === 'object' ? JSON.stringify(claim.object_value) : String(claim.object_value);
      }
      if (claim.object_entity_id) {
        const uri = 'mem://memory/' + claim.object_entity_id;
        const node = (this.graph.nodes || []).find((item) => item.source_uri === uri);
        return node ? this.displayName(node) : String(claim.object_entity_id);
      }
      return 'Unknown';
    },
    formatDate(value) {
      if (!value) return '';
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
    },
    isDocumentLike(node) { return ['DOCUMENT', 'EPISODE', 'SESSION_SUMMARY', 'COLLECTION'].includes(node && node.node_type); },
    documentNodes() {
      const term = this.sourceSearch.trim().toLowerCase();
      return (this.graph.nodes || []).filter((node) => {
        if (!this.isDocumentLike(node)) return false;
        return !term
          || this.sourceTitle(node).toLowerCase().includes(term)
          || this.displayName(node).toLowerCase().includes(term);
      }).slice(0, 40);
    },
    selectedConnections() {
      if (!this.selected) return [];
      const byId = new Map((this.graph.nodes || []).map((node) => [node.id, node]));
      const connections = new Map();
      (this.graph.edges || [])
        .filter((edge) => edge.source === this.selected.id || edge.target === this.selected.id)
        .forEach((edge) => {
          const outgoing = edge.source === this.selected.id;
          const connectedId = outgoing ? edge.target : edge.source;
          const rawLabel = edge.label || edge.type || 'related to';
          const key = `${outgoing ? 'outgoing' : 'incoming'}|${rawLabel}|${connectedId}`;
          if (!connections.has(key)) {
            connections.set(key, {
              label: this.humanize(rawLabel),
              direction: outgoing ? 'outgoing' : 'incoming',
              node: byId.get(connectedId),
            });
          }
        });
      return Array.from(connections.values()).slice(0, 25);
    },
    async focusNode(node) {
      if (!node || !node.source_uri) return;
      this.rootUri = node.source_uri; this.depth = 1; this.limit = 40;
      await this.load(node.source_uri);
    },
    clearSelection() { this.selected = null; this.selectedDetails = null; this.draw(); },
    resetView() { this.rootUri = ''; this.search = ''; this.sourceSearch = ''; this.nodeType = ''; this.statusFilter = ''; this.selected = null; this.selectedDetails = null; this.depth = 1; this.limit = 40; this.load(); },
    openMemory() { if (this.selected && this.selected.source_uri) window.location.href = '/admin/dashboard?tab=memories&prefix=' + encodeURIComponent(this.selected.source_uri); },
    _login() { window.location.href = '/admin/login'; },
    _authHeaders() { const token = localStorage.getItem('engram_api_key'); return token ? { Authorization: 'Bearer ' + token } : {}; },
  };
}
