(function () {
  if (window.__tinitiateDataControlLoaded) return;
  window.__tinitiateDataControlLoaded = true;

  const STYLE = `
    .tdc-header-btn {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: #0284c7;
      color: #ffffff !important;
      border: 1px solid rgba(255,255,255,0.28);
      border-radius: 999px;
      padding: 7px 14px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      font-family: inherit;
      line-height: 1.2;
      transition: background 0.15s ease, transform 0.12s ease;
    }
    .tdc-header-btn:hover { background: #0369a1; transform: translateY(-1px); }
    .tdc-hero-btn-wrap { margin-top: 18px; }
    .tdc-nav-item { display: flex; align-items: center; margin-left: 8px; }

    .tdc-backdrop {
      position: fixed; inset: 0; z-index: 99999;
      background: rgba(15, 23, 42, 0.62);
      display: none; align-items: center; justify-content: center;
      padding: 18px;
      font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
    }
    .tdc-backdrop.open { display: flex; }
    .tdc-modal {
      background: #f8fafc;
      color: #0f172a;
      width: 100%;
      max-width: 980px;
      max-height: 90vh;
      border-radius: 14px;
      box-shadow: 0 24px 60px rgba(2, 6, 23, 0.35);
      display: flex;
      flex-direction: column;
      overflow: hidden;
      border: 1px solid #cbd5e1;
    }
    .tdc-modal-head {
      background: linear-gradient(125deg, #0b2545 0%, #0f4c81 65%, #0284c7 100%);
      color: #fff;
      padding: 16px 22px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .tdc-modal-head h2 { margin: 0; font-size: 18px; font-weight: 700; }
    .tdc-modal-head p { margin: 2px 0 0; font-size: 12px; opacity: 0.85; }
    .tdc-close {
      background: rgba(255,255,255,0.16);
      color: #fff;
      border: none;
      border-radius: 8px;
      width: 34px; height: 34px;
      font-size: 20px;
      cursor: pointer;
    }
    .tdc-close:hover { background: rgba(255,255,255,0.28); }
    .tdc-body {
      padding: 18px 22px 22px;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 18px;
    }
    .tdc-panel {
      background: #ffffff;
      border: 1px solid #e2e8f0;
      border-radius: 12px;
      padding: 16px 18px;
    }
    .tdc-panel-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin: 0 0 12px;
      font-size: 15px;
      font-weight: 700;
      color: #0f172a;
    }
    .tdc-form-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 12px;
      align-items: end;
    }
    .tdc-field label {
      display: block;
      font-size: 12px;
      font-weight: 600;
      color: #475569;
      margin-bottom: 5px;
    }
    .tdc-field select, .tdc-field input {
      width: 100%;
      padding: 9px 11px;
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      font-size: 13px;
      background: #fff;
      color: #0f172a;
    }
    .tdc-btn-primary {
      background: #0f4c81;
      color: #fff;
      border: none;
      border-radius: 8px;
      padding: 10px 16px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
    }
    .tdc-btn-primary:hover { background: #0b3963; }
    .tdc-btn-primary:disabled { opacity: 0.6; cursor: not-allowed; }
    .tdc-btn-ghost {
      background: #e2e8f0;
      color: #0f172a;
      border: none;
      border-radius: 7px;
      padding: 6px 12px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }
    .tdc-btn-ghost:hover { background: #cbd5e1; }
    .tdc-status-msg {
      margin-top: 12px;
      padding: 10px 12px;
      border-radius: 8px;
      font-size: 13px;
      display: none;
    }
    .tdc-status-msg.ok { display: block; background: #ecfdf5; border: 1px solid #86efac; color: #166534; }
    .tdc-status-msg.err { display: block; background: #fef2f2; border: 1px solid #fca5a5; color: #991b1b; }

    .tdc-cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    .tdc-card {
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      padding: 12px 14px;
    }
    .tdc-card-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 6px;
      font-size: 13px;
      font-weight: 700;
    }
    .tdc-badge {
      font-size: 11px;
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 999px;
    }
    .tdc-badge.ok { background: #dcfce7; color: #166534; }
    .tdc-badge.err { background: #fee2e2; color: #991b1b; }
    .tdc-metric { font-size: 22px; font-weight: 800; color: #0f4c81; }
    .tdc-sub { font-size: 12px; color: #64748b; }

    .tdc-tabs { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
    .tdc-tab {
      background: #e2e8f0;
      color: #334155;
      border: none;
      border-radius: 7px;
      padding: 6px 12px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }
    .tdc-tab.active { background: #0f4c81; color: #fff; }
    .tdc-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    .tdc-table th, .tdc-table td {
      text-align: left;
      padding: 8px 10px;
      border-bottom: 1px solid #e2e8f0;
    }
    .tdc-table th { background: #f1f5f9; font-size: 12px; color: #475569; }
    .tdc-num { font-weight: 700; color: #0f4c81; }
  `;

  let latestReport = null;
  let activeTab = "postgres";

  const PG_DEFAULT_TABLES = ["categories", "customers", "products", "orders", "order_items", "payments", "inventory"];
  const MONGO_DEFAULT_COLLS = ["customers_100", "organizations_100", "people_100", "products_100", "bank_statements"];
  const MINIO_DEFAULT_PREFIXES = ["custom-uploads", "business-data-dumps", "file-formats/csv"];

  function injectStyles() {
    const st = document.createElement("style");
    st.textContent = STYLE;
    document.head.appendChild(st);
  }

  function createButton(label) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "tdc-header-btn";
    btn.innerHTML = `<span>&#9881;</span><span>${label || "Data Control"}</span>`;
    btn.addEventListener("click", openModal);
    return btn;
  }

  function injectHeaderButton() {
    const navUl = document.querySelector("header.site nav ul");
    if (navUl) {
      const li = document.createElement("li");
      li.className = "tdc-nav-item";
      li.appendChild(createButton("Data Control"));
      navUl.appendChild(li);
      return;
    }
    const heroWrap = document.querySelector("header.hero .wrap");
    if (heroWrap) {
      const wrap = document.createElement("div");
      wrap.className = "tdc-hero-btn-wrap";
      wrap.appendChild(createButton("Data Control & Reporting"));
      heroWrap.appendChild(wrap);
    }
  }

  function buildModal() {
    const backdrop = document.createElement("div");
    backdrop.id = "tdc-backdrop";
    backdrop.className = "tdc-backdrop";
    backdrop.innerHTML = `
      <div class="tdc-modal" role="dialog" aria-modal="true" aria-labelledby="tdc-title">
        <div class="tdc-modal-head">
          <div>
            <h2 id="tdc-title">Docker Data Control &amp; Reporting</h2>
            <p>Live status across PostgreSQL, MongoDB, and MinIO + interactive data posting</p>
          </div>
          <button type="button" class="tdc-close" id="tdc-close-btn" aria-label="Close">&times;</button>
        </div>
        <div class="tdc-body">
          <div class="tdc-panel">
            <div class="tdc-panel-title">
              <span>Post Data to Storage</span>
            </div>
            <form id="tdc-post-form" class="tdc-form-grid">
              <div class="tdc-field">
                <label for="tdc-store">1. Target Store</label>
                <select id="tdc-store" required>
                  <option value="postgres">PostgreSQL (RDBMS)</option>
                  <option value="mongodb">MongoDB (NoSQL)</option>
                  <option value="minio">MinIO (Object Storage)</option>
                </select>
              </div>
              <div class="tdc-field">
                <label for="tdc-target" id="tdc-target-label">2. Target Table</label>
                <select id="tdc-target" required></select>
              </div>
              <div class="tdc-field">
                <label for="tdc-rows">3. How Many Rows</label>
                <input id="tdc-rows" type="number" min="1" max="500" value="10" required />
              </div>
              <div class="tdc-field">
                <button type="submit" class="tdc-btn-primary" id="tdc-submit-btn">Post Data</button>
              </div>
            </form>
            <div id="tdc-post-status" class="tdc-status-msg"></div>
          </div>

          <div class="tdc-panel">
            <div class="tdc-panel-title">
              <span>Live Data Status &amp; Reporting</span>
              <button type="button" class="tdc-btn-ghost" id="tdc-refresh-btn">&#8635; Refresh Report</button>
            </div>
            <div class="tdc-cards" id="tdc-cards"></div>
            <div class="tdc-tabs">
              <button type="button" class="tdc-tab active" data-tab="postgres">PostgreSQL Tables</button>
              <button type="button" class="tdc-tab" data-tab="mongodb">MongoDB Collections</button>
              <button type="button" class="tdc-tab" data-tab="minio">MinIO Buckets &amp; Prefixes</button>
              <button type="button" class="tdc-tab" data-tab="kafka">Kafka Queue Jobs</button>
            </div>
            <div id="tdc-table-wrap">Loading live report...</div>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(backdrop);

    backdrop.addEventListener("click", (e) => {
      if (e.target === backdrop) closeModal();
    });
    document.getElementById("tdc-close-btn").addEventListener("click", closeModal);
    document.getElementById("tdc-refresh-btn").addEventListener("click", loadReport);
    document.getElementById("tdc-store").addEventListener("change", updateTargetOptions);
    document.getElementById("tdc-post-form").addEventListener("submit", handlePostData);

    backdrop.querySelectorAll(".tdc-tab").forEach((tabBtn) => {
      tabBtn.addEventListener("click", () => {
        activeTab = tabBtn.dataset.tab;
        backdrop.querySelectorAll(".tdc-tab").forEach((b) => b.classList.toggle("active", b === tabBtn));
        renderTable();
      });
    });

    updateTargetOptions();
  }

  function updateTargetOptions() {
    const store = document.getElementById("tdc-store").value;
    const targetSelect = document.getElementById("tdc-target");
    const label = document.getElementById("tdc-target-label");
    let options = [];

    if (store === "postgres") {
      label.textContent = "2. Target PostgreSQL Table";
      const liveTables = (latestReport?.postgres?.tables || [])
        .map((t) => t.name)
        .filter((n) => n !== "etl_batch_control");
      options = liveTables.length ? liveTables : PG_DEFAULT_TABLES;
    } else if (store === "mongodb") {
      label.textContent = "2. Target MongoDB Collection";
      const liveColls = (latestReport?.mongodb?.collections || []).map((c) => c.name);
      options = Array.from(new Set([...liveColls, ...MONGO_DEFAULT_COLLS]));
    } else {
      label.textContent = "2. Target MinIO Folder / Prefix";
      const livePrefixes = (latestReport?.minio?.prefixes || [])
        .map((p) => p.prefix)
        .filter((p) => p && p !== "(root)" && p !== "README.md");
      options = Array.from(new Set([...MINIO_DEFAULT_PREFIXES, ...livePrefixes]));
    }

    targetSelect.innerHTML = options.map((o) => `<option value="${o}">${o}</option>`).join("");
  }

  async function loadReport() {
    const tableWrap = document.getElementById("tdc-table-wrap");
    try {
      const resp = await fetch("/api/report");
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      latestReport = await resp.json();
      renderCards();
      renderTable();
      updateTargetOptions();
    } catch (err) {
      tableWrap.innerHTML = `<div class="tdc-status-msg err" style="display:block">Failed to load report: ${err.message}</div>`;
    }
  }

  function renderCards() {
    if (!latestReport) return;
    const pg = latestReport.postgres || {};
    const mg = latestReport.mongodb || {};
    const mn = latestReport.minio || {};
    const kf = latestReport.kafka || {};
    document.getElementById("tdc-cards").innerHTML = `
      <div class="tdc-card">
        <div class="tdc-card-head">
          <span>PostgreSQL (${pg.database || "ai_setup"})</span>
          <span class="tdc-badge ${pg.status === "connected" ? "ok" : "err"}">${pg.status || "unknown"}</span>
        </div>
        <div class="tdc-metric">${(pg.total_rows ?? 0).toLocaleString()} rows</div>
        <div class="tdc-sub">${(pg.tables || []).length} tables &middot; Structured RDBMS</div>
      </div>
      <div class="tdc-card">
        <div class="tdc-card-head">
          <span>MongoDB (${mg.database || "ai_setup"})</span>
          <span class="tdc-badge ${mg.status === "connected" ? "ok" : "err"}">${mg.status || "unknown"}</span>
        </div>
        <div class="tdc-metric">${(mg.total_documents ?? 0).toLocaleString()} docs</div>
        <div class="tdc-sub">${(mg.collections || []).length} collections &middot; NoSQL JSON</div>
      </div>
      <div class="tdc-card">
        <div class="tdc-card-head">
          <span>MinIO (${mn.bucket || "ai-source-data"})</span>
          <span class="tdc-badge ${mn.status === "connected" ? "ok" : "err"}">${mn.status || "unknown"}</span>
        </div>
        <div class="tdc-metric">${(mn.total_objects ?? 0).toLocaleString()} files</div>
        <div class="tdc-sub">${(mn.total_size_kb ?? 0).toLocaleString()} KB &middot; S3 Object Storage</div>
      </div>
      <div class="tdc-card">
        <div class="tdc-card-head">
          <span>Kafka (${kf.topic || "tinitiate-data-ingest"})</span>
          <span class="tdc-badge ${kf.status === "connected" ? "ok" : "err"}">${kf.status || "unknown"}</span>
        </div>
        <div class="tdc-metric">${(kf.recent_job_count ?? 0).toLocaleString()} jobs</div>
        <div class="tdc-sub">SASL_PLAINTEXT &middot; Message Queue</div>
      </div>
    `;
  }

  function renderTable() {
    if (!latestReport) return;
    const wrap = document.getElementById("tdc-table-wrap");
    if (activeTab === "postgres") {
      const rows = latestReport.postgres?.tables || [];
      wrap.innerHTML = `
        <table class="tdc-table">
          <thead><tr><th>Table Name</th><th>Row Count</th><th>Data Type</th></tr></thead>
          <tbody>
            ${rows.map((r) => `<tr><td><code>${r.name}</code></td><td class="tdc-num">${r.rows.toLocaleString()}</td><td>${r.data_type}</td></tr>`).join("")}
          </tbody>
        </table>
      `;
    } else if (activeTab === "mongodb") {
      const rows = latestReport.mongodb?.collections || [];
      wrap.innerHTML = `
        <table class="tdc-table">
          <thead><tr><th>Collection Name</th><th>Document Count</th><th>Data Type</th></tr></thead>
          <tbody>
            ${rows.map((r) => `<tr><td><code>${r.name}</code></td><td class="tdc-num">${r.documents.toLocaleString()}</td><td>${r.data_type}</td></tr>`).join("")}
          </tbody>
        </table>
      `;
    } else if (activeTab === "minio") {
      const rows = latestReport.minio?.prefixes || [];
      wrap.innerHTML = `
        <table class="tdc-table">
          <thead><tr><th>Folder / Prefix</th><th>Objects</th><th>Size (KB)</th><th>File Types</th></tr></thead>
          <tbody>
            ${rows.map((r) => `<tr><td><code>${r.prefix}</code></td><td class="tdc-num">${r.objects.toLocaleString()}</td><td>${r.size_kb.toLocaleString()}</td><td>${r.data_type}</td></tr>`).join("")}
          </tbody>
        </table>
      `;
    } else {
      const jobs = latestReport.kafka?.jobs || [];
      wrap.innerHTML = jobs.length
        ? `<table class="tdc-table">
            <thead><tr><th>Job ID</th><th>Store</th><th>Target</th><th>Rows</th><th>Status</th><th>Kafka Offset</th></tr></thead>
            <tbody>
              ${jobs.map((j) => `<tr><td><code>${j.job_id}</code></td><td>${j.store}</td><td><code>${j.target}</code></td><td class="tdc-num">${j.rows}</td><td>${j.status}</td><td>p${j.kafka_partition ?? 0} @ ${j.kafka_offset ?? "-"}</td></tr>`).join("")}
            </tbody>
          </table>`
        : `<div class="tdc-sub">No Kafka ingest jobs queued yet. Use the Post Data form above to queue one.</div>`;
    }
  }

  async function handlePostData(e) {
    e.preventDefault();
    const store = document.getElementById("tdc-store").value;
    const target = document.getElementById("tdc-target").value;
    const rows = parseInt(document.getElementById("tdc-rows").value, 10);
    const btn = document.getElementById("tdc-submit-btn");
    const msg = document.getElementById("tdc-post-status");

    btn.disabled = true;
    btn.textContent = "Queueing to Kafka...";
    msg.className = "tdc-status-msg";
    msg.textContent = "";

    try {
      const resp = await fetch("/api/post-data", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ store, target, rows }),
      });
      const queued = await resp.json();
      if (!resp.ok) throw new Error(queued.detail || `HTTP ${resp.status}`);

      msg.textContent = `Queued job ${queued.job_id} to Kafka (${queued.kafka_topic}, offset ${queued.kafka_offset ?? 0}). Waiting for worker...`;
      msg.className = "tdc-status-msg ok";

      let job = queued;
      for (let i = 0; i < 30; i++) {
        if (job.status === "completed" || job.status === "failed") break;
        await new Promise((r) => setTimeout(r, 400));
        const jResp = await fetch(`/api/jobs/${queued.job_id}`);
        if (jResp.ok) job = await jResp.json();
      }

      if (job.status === "failed") {
        throw new Error(job.error || "Kafka worker failed to process job.");
      }

      if (store === "minio") {
        msg.textContent = `Kafka job ${job.job_id} completed: uploaded CSV with ${job.rows_in_csv} rows to ${job.object_key}.`;
      } else {
        msg.textContent = `Kafka job ${job.job_id} completed (offset ${job.kafka_offset ?? 0}): posted ${job.inserted_count} record(s) into ${store.toUpperCase()} -> ${job.target}.`;
      }
      msg.className = "tdc-status-msg ok";
      activeTab = store;
      document.querySelectorAll(".tdc-tab").forEach((b) => b.classList.toggle("active", b.dataset.tab === store));
      await loadReport();
    } catch (err) {
      msg.textContent = `Error posting data: ${err.message}`;
      msg.className = "tdc-status-msg err";
    } finally {
      btn.disabled = false;
      btn.textContent = "Post Data";
    }
  }

  function openModal() {
    document.getElementById("tdc-backdrop").classList.add("open");
    loadReport();
  }

  function closeModal() {
    document.getElementById("tdc-backdrop").classList.remove("open");
  }

  document.addEventListener("DOMContentLoaded", () => {
    injectStyles();
    injectHeaderButton();
    buildModal();
  });
})();

