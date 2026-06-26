    function dashboard() {
        return {
            // Theme
            theme: localStorage.getItem('engram-admin-theme') || 'auto',
            activeTheme: 'light',
            systemThemeListener: null,

            // Mobile menu
            mobileMenuOpen: false,

            // Main tab state (Status, Sessions, Memories, Ingest, Settings)
            mainTab: 'status',

            // Theme dropdown
            themeDropdown: false,

            // Status tab state
            healthStatus: 'unknown',
            healthComponents: {},
            kgNodes: '-',
            kgEdges: '-',
            queueDepth: '-',
            pendingEvents: 0,
            processingEvents: 0,
            outboxPending: 0,
            _healthTimer: null,
            _pipelineTimer: null,

            // Sessions tab state
            sessions: [],
            loadingSessions: false,
            _sessionsTimer: null,

            // Memories tab state
            memories: [],
            memSearch: '',
            loadingMemories: false,

            // Ingest tab state
            ingest: { session_id: '', user: '', assistant: '' },
            ingestStatus: '',
            bulk: { file: null, dryRun: false, sessionId: '', fileFormat: '' },
            bulkStatus: '',
            bulkJob: null,

            // Settings tab state
            language: localStorage.getItem('engram-admin-lang') || 'en',

            // Shared
            apiKey: localStorage.getItem('engram_api_key') || '',

            async init() {
                document.documentElement.setAttribute('data-theme', 'light');
                this.applyTabStateFromUrl();

                // Set initial tab
                await this.handleMainTabChange(this.mainTab);

                this.$watch('mainTab', (value) => {
                    this.handleMainTabChange(value);
                });

                window.addEventListener('popstate', () => {
                    this.applyTabStateFromUrl();
                });

            document.addEventListener('visibilitychange', () => {
                if (document.hidden) {
                    this.stopHealthTimer();
                    this.stopSessionsTimer();
                    this.stopPipelineTimer();
                } else {
                    if (this.mainTab === 'status') {
                        this.startHealthTimer();
                        this.startPipelineTimer();
                    }
                    if (this.mainTab === 'sessions') this.startSessionsTimer();
                }
            });
            },

            async handleMainTabChange(value) {
                if (value === 'status') {
                    await this.pollHealth();
                    this.startHealthTimer();
                    await this.pollPipeline();
                    this.startPipelineTimer();
                } else {
                    this.stopHealthTimer();
                    this.stopPipelineTimer();
                }
                if (value === 'sessions') {
                    await this.loadSessions();
                    this.startSessionsTimer();
                } else {
                    this.stopSessionsTimer();
                }
                if (value === 'memories') {
                    await this.searchMemories();
                }
                if (value === 'ingest') {
                    // nothing to preload
                }
            },

            applyTabStateFromUrl() {
                const params = new URLSearchParams(window.location.search);
                const mainTab = params.get('tab');
                const valid = new Set(['status', 'sessions', 'memories', 'ingest', 'settings']);
                this.mainTab = valid.has(mainTab) ? mainTab : 'status';
                const prefix = params.get('prefix');
                if (prefix) this.memSearch = prefix;
            },

            syncTabStateToUrl() {
                const url = new URL(window.location.href);
                url.searchParams.set('tab', this.mainTab);
                window.history.replaceState({}, '', url);
            },

            setMainTab(tab) {
                const valid = new Set(['status', 'sessions', 'memories', 'ingest', 'settings']);
                if (!valid.has(tab)) return;
                this.mainTab = tab;
                this.syncTabStateToUrl();
            },

            // ========== Theme ==========
            applyTheme() {
                const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
                const isDark = this.theme === 'dark' || (this.theme === 'auto' && prefersDark);
                this.activeTheme = isDark ? 'dark' : 'light';
                document.documentElement.setAttribute('data-theme', this.activeTheme);
                if (this.systemThemeListener) {
                    window.matchMedia('(prefers-color-scheme: dark)').removeEventListener('change', this.systemThemeListener);
                }
                if (this.theme === 'auto') {
                    this.systemThemeListener = (e) => {
                        this.activeTheme = e.matches ? 'dark' : 'light';
                        document.documentElement.setAttribute('data-theme', this.activeTheme);
                    };
                    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', this.systemThemeListener);
                }
            },

            setTheme(mode) {
                this.theme = mode;
                localStorage.setItem('engram-admin-theme', mode);
                this.applyTheme();
            },

            // ========== Health / Status ==========
            async pollHealth() {
                try {
                    const response = await fetch('/api/v1/health', {
                        headers: this._authHeaders(),
                    });
                    if (response.ok) {
                        const data = await response.json();
                        this.healthStatus = data.status || 'unknown';
                        this.healthComponents = data.components || {};
                    } else if (response.status === 401) {
                        window.location.href = '/admin/login';
                    }
                } catch (err) {
                    this.healthStatus = 'unavailable';
                }

                try {
                    const r = await fetch('/admin/api/stats', { headers: this._authHeaders() });
                    if (r.ok) {
                        const data = await r.json();
                        this.kgNodes = data.kg_nodes ?? '-';
                        this.kgEdges = data.kg_edges ?? '-';
                        this.queueDepth = data.queue_depth ?? '-';
                    }
                } catch (_) {
                    this.kgNodes = '-';
                    this.kgEdges = '-';
                    this.queueDepth = '-';
                }
            },

            startHealthTimer() {
                this.stopHealthTimer();
                this._healthTimer = setInterval(() => this.pollHealth(), 5000);
            },

            stopHealthTimer() {
                if (this._healthTimer) {
                    clearInterval(this._healthTimer);
                    this._healthTimer = null;
                }
            },

            async pollPipeline() {
                try {
                    const r = await fetch('/admin/api/pipeline', { headers: this._authHeaders() });
                    if (r.ok) {
                        const data = await r.json();
                        this.pendingEvents = data.pending_events ?? 0;
                        this.processingEvents = data.processing_events ?? 0;
                        this.outboxPending = data.outbox_pending ?? 0;
                    }
                } catch (_) {
                    this.pendingEvents = 0;
                    this.processingEvents = 0;
                    this.outboxPending = 0;
                }
            },

            startPipelineTimer() {
                this.stopPipelineTimer();
                this._pipelineTimer = setInterval(() => this.pollPipeline(), 3000);
            },

            stopPipelineTimer() {
                if (this._pipelineTimer) {
                    clearInterval(this._pipelineTimer);
                    this._pipelineTimer = null;
                }
            },

            // ========== Sessions ==========
            async loadSessions() {
                this.loadingSessions = true;
                try {
                    const response = await fetch('/admin/api/sessions', {
                        headers: this._authHeaders(),
                    });
                    if (response.ok) {
                        const data = await response.json();
                        this.sessions = data.sessions || [];
                    } else if (response.status === 401) {
                        window.location.href = '/admin/login';
                    }
                } catch (err) {
                    console.error('Failed to load sessions:', err);
                } finally {
                    this.loadingSessions = false;
                }
            },

            async createSession() {
                try {
                    const response = await fetch('/admin/api/sessions', {
                        method: 'POST',
                        headers: this._authHeaders(),
                    });
                    if (response.ok) {
                        const data = await response.json();
                        this.ingest.session_id = data.session_id;
                        await this.loadSessions();
                    } else if (response.status === 401) {
                        window.location.href = '/admin/login';
                    }
                } catch (err) {
                    console.error('Failed to create session:', err);
                }
            },

            startSessionsTimer() {
                this.stopSessionsTimer();
                this._sessionsTimer = setInterval(() => this.loadSessions(), 10000);
            },

            stopSessionsTimer() {
                if (this._sessionsTimer) {
                    clearInterval(this._sessionsTimer);
                    this._sessionsTimer = null;
                }
            },

            // ========== Memories ==========
            async searchMemories() {
                this.loadingMemories = true;
                try {
                    const params = new URLSearchParams();
                    if (this.memSearch.trim()) {
                        params.set('prefix', this.memSearch.trim());
                    }
                    params.set('limit', '50');
                    const response = await fetch('/admin/api/memories?' + params.toString(), {
                        headers: this._authHeaders(),
                    });
                    if (response.ok) {
                        const data = await response.json();
                        this.memories = (data.items || []).map(item => ({
                            uri: item.source_uri,
                            content: item.l0_abstract || item.body || '',
                            type: item.node_type || 'memory',
                            created_at: item.created_at || new Date().toISOString(),
                        }));
                    } else if (response.status === 401) {
                        window.location.href = '/admin/login';
                    }
                } catch (err) {
                    console.error('Failed to search memories:', err);
                } finally {
                    this.loadingMemories = false;
                }
            },

            // ========== Ingest ==========
            async submitIngest() {
                const oldStatus = this.ingestStatus;
                if (!this.ingest.user.trim() && !this.ingest.assistant.trim()) {
                    this.ingestStatus = 'Please enter a message.';
                    return;
                }
                this.ingestStatus = 'Submitting...';
                try {
                    const payload = {
                        source: 'admin_ui',
                        turn_pair: {
                            user: { turn_idx: 0, content: this.ingest.user.trim() },
                            assistant: { turn_idx: 1, content: this.ingest.assistant.trim() },
                        },
                    };
                    if (this.ingest.session_id.trim()) {
                        payload.session_id = this.ingest.session_id.trim();
                    }
                    const response = await fetch('/admin/api/ingest', {
                        method: 'POST',
                        headers: { ...this._authHeaders(), 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload),
                    });
                    if (response.ok) {
                        const data = await response.json();
                        this.ingestStatus = 'Ingested: ' + data.event_id;
                        this.ingest.user = '';
                        this.ingest.assistant = '';
                        this.pollPipeline();
                        this.pollHealth();
                    } else if (response.status === 401) {
                        window.location.href = '/admin/login';
                    } else {
                        const data = await response.json().catch(() => ({}));
                        this.ingestStatus = 'Error: ' + (data.detail || response.statusText);
                    }
                } catch (err) {
                    this.ingestStatus = 'Error: ' + err.message;
                }
            },

            async submitBulkUpload() {
                if (!this.bulk.file) {
                    this.bulkStatus = 'Choose a JSONL, CSV, or ZIP file.';
                    return;
                }
                this.bulkStatus = 'Uploading...';
                this.bulkJob = null;
                const form = new FormData();
                form.append('file', this.bulk.file);
                form.append('dry_run', this.bulk.dryRun ? 'true' : 'false');
                if (this.bulk.sessionId.trim()) form.append('session_id', this.bulk.sessionId.trim());
                if (this.bulk.fileFormat) form.append('file_format', this.bulk.fileFormat);
                try {
                    const response = await fetch('/admin/api/ingest/bulk', {
                        method: 'POST',
                        headers: this._authHeaders(),
                        body: form,
                    });
                    if (response.ok) {
                        const data = await response.json();
                        this.bulkJob = data;
                        this.bulkStatus = `${data.status}: ${data.accepted_count} accepted, ${data.rejected_count} rejected`;
                        if (!data.dry_run && data.job_id) this.pollBulkJob(data.job_id);
                        this.pollPipeline();
                    } else if (response.status === 401) {
                        window.location.href = '/admin/login';
                    } else {
                        const data = await response.json().catch(() => ({}));
                        this.bulkStatus = 'Error: ' + (data.detail || response.statusText);
                    }
                } catch (err) {
                    this.bulkStatus = 'Error: ' + err.message;
                }
            },

            async pollBulkJob(jobId) {
                try {
                    const response = await fetch('/admin/api/ingest/bulk/' + encodeURIComponent(jobId), {
                        headers: this._authHeaders(),
                    });
                    if (!response.ok) return;
                    const data = await response.json();
                    this.bulkJob = data;
                    this.bulkStatus = `${data.status}: ${data.accepted_count} accepted, ${data.rejected_count} rejected`;
                } catch (_) {
                    // best-effort status refresh
                }
            },

            // ========== Settings ==========
            saveLanguage() {
                localStorage.setItem('engram-admin-lang', this.language);
                location.reload();
            },

            clearSession() {
                document.cookie = 'engram_session_token=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT;';
                localStorage.removeItem('engram_api_key');
                window.location.href = '/admin/login';
            },

            // ========== Shared helpers ==========
            _authHeaders() {
                const headers = {};
                const token = localStorage.getItem('engram_api_key');
                if (token) {
                    headers['Authorization'] = 'Bearer ' + token;
                }
                return headers;
            },

            async logout() {
                try {
                    await fetch('/admin/api/logout', { method: 'POST' });
                } catch (err) {
                    // ignore
                } finally {
                    this.clearSession();
                }
            },

            formatDate(iso) {
                if (!iso) return '—';
                const d = new Date(iso);
                return d.toLocaleString();
            },
        };
    }
