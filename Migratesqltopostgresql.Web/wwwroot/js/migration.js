// ── main page refs ────────────────────────────────────────────────────────────
const dbNameInput  = document.getElementById("dbname");
const targetInput  = document.getElementById("targetDbname");
const migrateBtn   = document.getElementById("migrateBtn");
const progressBar  = document.getElementById("progressBar");
const progressText = document.getElementById("progressText");
const statusText   = document.getElementById("statusText");
const errorText    = document.getElementById("errorText");
const logBox       = document.getElementById("logBox");

// ── modal refs ────────────────────────────────────────────────────────────────
const connModal                    = document.getElementById("connModal");
const connModalError               = document.getElementById("connModalError");
const stepSqlServer                = document.getElementById("stepSqlServer");
const stepPostgres                 = document.getElementById("stepPostgres");
const sqlServerTemplateInput       = document.getElementById("sqlServerTemplate");
const postgresAdminConnectionInput = document.getElementById("postgresAdminConnection");
const connCancelBtn                = document.getElementById("connCancelBtn");
const connBackBtn                  = document.getElementById("connBackBtn");
const connNextBtn                  = document.getElementById("connNextBtn");
const connStartBtn                 = document.getElementById("connStartBtn");
const sqlConnectBtn                = document.getElementById("sqlConnectBtn");
const sqlConnStatus                = document.getElementById("sqlConnStatus");
const pgConnectBtn                 = document.getElementById("pgConnectBtn");
const pgConnStatus                 = document.getElementById("pgConnStatus");

let pollHandle = null;
let sqlVerified = false;
let pgVerified  = false;

// ── localStorage helpers ──────────────────────────────────────────────────────
function loadSavedConnections() {
  try {
    const sqlConn = localStorage.getItem('migration_sqlserver_conn');
    const pgConn = localStorage.getItem('migration_postgres_conn');

    if (sqlConn) {
      sqlServerTemplateInput.value = sqlConn;
    }
    if (pgConn) {
      postgresAdminConnectionInput.value = pgConn;
    }
  } catch (err) {
    console.warn('Could not load saved connections:', err);
  }
}

function saveConnections() {
  try {
    const sqlConn = sqlServerTemplateInput.value.trim();
    const pgConn = postgresAdminConnectionInput.value.trim();

    if (sqlConn) {
      localStorage.setItem('migration_sqlserver_conn', sqlConn);
    }
    if (pgConn) {
      localStorage.setItem('migration_postgres_conn', pgConn);
    }
  } catch (err) {
    console.warn('Could not save connections:', err);
  }
}

// ── helpers ───────────────────────────────────────────────────────────────────
function setProgress(value) {
  const n = Math.max(0, Math.min(100, Number(value) || 0));
  progressBar.style.width = `${n}%`;
  progressText.textContent = `${n}%`;
}

function setRunning(on) {
  migrateBtn.disabled = on;
  dbNameInput.disabled = on;
  targetInput.disabled = on;
}

function showLogs(logs) {
  if (!Array.isArray(logs)) return;
  logBox.textContent = logs.join("\n");
  logBox.scrollTop = logBox.scrollHeight;
}

function setConnStatus(el, state, message) {
  // state: '' | 'testing' | 'ok' | 'fail'
  el.className = `conn-status ${state}`.trim();
  el.textContent = state === "testing" ? "Testing…" : (message || "");
}

// ── polling ───────────────────────────────────────────────────────────────────
function beginPolling(jobId) {
  if (pollHandle) clearInterval(pollHandle);

  pollHandle = setInterval(async () => {
    try {
      const res = await fetch(`/api/migration/status/${jobId}`);
      if (!res.ok) throw new Error("Unable to fetch status.");
      const data = await res.json();

      setProgress(data.progress);
      statusText.textContent = data.status || "Running";
      showLogs(data.logs || []);

      if (data.done) {
        clearInterval(pollHandle);
        pollHandle = null;
        setRunning(false);
        if (data.success) {
          errorText.textContent = "";
          statusText.textContent = "Migration completed.";
          setProgress(100);
        } else {
          statusText.textContent = "Migration failed.";
          errorText.textContent = data.error || "Migration failed and rolled back.";
        }
      }
    } catch (err) {
      clearInterval(pollHandle);
      pollHandle = null;
      setRunning(false);
      statusText.textContent = "Error";
      errorText.textContent = err?.message || "Status polling failed.";
    }
  }, 1000);
}

// ── modal open / close ────────────────────────────────────────────────────────
function openConnModal() {
  sqlVerified = false;
  pgVerified  = false;
  connModalError.textContent = "";

  // Load saved connections from localStorage
  loadSavedConnections();

  setConnStatus(sqlConnStatus, "", "");
  connNextBtn.disabled = true;
  sqlConnectBtn.disabled = false;

  setConnStatus(pgConnStatus, "", "");
  connStartBtn.disabled = true;
  pgConnectBtn.disabled = false;

  stepSqlServer.classList.remove("hidden");
  stepPostgres.classList.add("hidden");
  connBackBtn.classList.add("hidden");
  connNextBtn.classList.remove("hidden");
  connStartBtn.classList.add("hidden");

  connModal.classList.remove("hidden");
  sqlServerTemplateInput.focus();
}

function closeConnModal() {
  connModal.classList.add("hidden");
}

function goToPostgresStep() {
  connModalError.textContent = "";
  stepSqlServer.classList.add("hidden");
  stepPostgres.classList.remove("hidden");
  connBackBtn.classList.remove("hidden");
  connNextBtn.classList.add("hidden");
  connStartBtn.classList.remove("hidden");
  postgresAdminConnectionInput.focus();
}

function goToSqlStep() {
  connModalError.textContent = "";
  stepSqlServer.classList.remove("hidden");
  stepPostgres.classList.add("hidden");
  connBackBtn.classList.add("hidden");
  connNextBtn.classList.remove("hidden");
  connStartBtn.classList.add("hidden");
}

// ── test connection helper ────────────────────────────────────────────────────
async function testConnection(type, connectionString, statusEl, connectBtn) {
  if (!connectionString) {
    setConnStatus(statusEl, "fail", "Please enter a connection string first.");
    return false;
  }

  connectBtn.disabled = true;
  setConnStatus(statusEl, "testing", "");

  try {
    const res = await fetch("/api/migration/test-connection", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type, connectionString })
    });
    const data = await res.json();

    if (data.success) {
      setConnStatus(statusEl, "ok", data.message);
      // Save successful connection to localStorage
      saveConnections();
      return true;
    } else {
      setConnStatus(statusEl, "fail", data.message);
      connectBtn.disabled = false;
      return false;
    }
  } catch (err) {
    setConnStatus(statusEl, "fail", err?.message || "Connection test failed.");
    connectBtn.disabled = false;
    return false;
  }
}

// ── test SQL Server ───────────────────────────────────────────────────────────
sqlConnectBtn.addEventListener("click", async () => {
  const ok = await testConnection(
    "sqlserver",
    sqlServerTemplateInput.value.trim(),
    sqlConnStatus,
    sqlConnectBtn
  );
  sqlVerified = ok;
  connNextBtn.disabled = !ok;
});

// ── test PostgreSQL ───────────────────────────────────────────────────────────
pgConnectBtn.addEventListener("click", async () => {
  const ok = await testConnection(
    "postgres",
    postgresAdminConnectionInput.value.trim(),
    pgConnStatus,
    pgConnectBtn
  );
  pgVerified = ok;
  connStartBtn.disabled = !ok;
});

// ── reset verified state when user edits ─────────────────────────────────────
sqlServerTemplateInput.addEventListener("input", () => {
  if (sqlVerified) {
    sqlVerified = false;
    connNextBtn.disabled = true;
    sqlConnectBtn.disabled = false;
    setConnStatus(sqlConnStatus, "", "");
  }
});

postgresAdminConnectionInput.addEventListener("input", () => {
  if (pgVerified) {
    pgVerified = false;
    connStartBtn.disabled = true;
    pgConnectBtn.disabled = false;
    setConnStatus(pgConnStatus, "", "");
  }
});

// ── kick off migration ────────────────────────────────────────────────────────
async function startMigration() {
  const dbName = dbNameInput.value.trim();
  if (!dbName) {
    errorText.textContent = "Please enter source DB name.";
    return;
  }
  openConnModal();
}

async function submitMigration() {
  if (!sqlVerified || !pgVerified) {
    connModalError.textContent = "Please test both connections before starting.";
    return;
  }

  const dbName   = dbNameInput.value.trim();
  const targetDb = targetInput.value.trim();
  const sqlCs    = sqlServerTemplateInput.value.trim();
  const pgCs     = postgresAdminConnectionInput.value.trim();

  // Get selected migration mode
  const migrationMode = document.querySelector('input[name="migrationMode"]:checked')?.value || 'schemaAndData';

  closeConnModal();
  setRunning(true);
  setProgress(0);
  statusText.textContent = "Submitting migration job…";
  errorText.textContent  = "";
  logBox.textContent     = "";

  try {
    const res = await fetch("/api/migration/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dbName,
        targetDbName: targetDb || null,
        sqlServerConnectionTemplate: sqlCs,
        postgresAdminConnection: pgCs,
        migrationMode: migrationMode
      })
    });

    if (!res.ok) {
      const body = await res.json().catch(() => ({ message: "Failed to start migration." }));
      throw new Error(body.message || "Failed to start migration.");
    }

    const body = await res.json();
    beginPolling(body.jobId);
  } catch (err) {
    setRunning(false);
    statusText.textContent = "Failed to start";
    errorText.textContent  = err?.message || "Failed to start migration.";
  }
}

// ── button wiring ─────────────────────────────────────────────────────────────
migrateBtn.addEventListener("click", startMigration);
connCancelBtn.addEventListener("click", closeConnModal);
connNextBtn.addEventListener("click", () => { if (sqlVerified) goToPostgresStep(); });
connBackBtn.addEventListener("click", goToSqlStep);
connStartBtn.addEventListener("click", submitMigration);
connModal.addEventListener("click", (e) => { if (e.target === connModal) closeConnModal(); });

// ── database comparison ───────────────────────────────────────────────────────
const compareBtn = document.getElementById("compareBtn");
const clearCompareBtn = document.getElementById("clearCompareBtn");
const comparisonResults = document.getElementById("comparisonResults");

compareBtn.addEventListener("click", async () => {
  const dbName = dbNameInput.value.trim();
  const targetDb = targetInput.value.trim() || dbName;
  const sqlCs = sqlServerTemplateInput.value.trim();
  const pgCs = postgresAdminConnectionInput.value.trim();

  if (!dbName || !sqlCs || !pgCs) {
    alert("Please enter database names and configure connections first.");
    return;
  }

  compareBtn.disabled = true;
  compareBtn.textContent = "Comparing...";
  comparisonResults.innerHTML = "<p class='loading'>Comparing databases...</p>";
  comparisonResults.classList.remove("hidden");
  clearCompareBtn.classList.add("hidden");

  try {
    const res = await fetch("/api/migration/compare", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        sourceDbName: dbName,
        targetDbName: targetDb,
        sqlServerConnectionTemplate: sqlCs,
        postgresAdminConnection: pgCs
      })
    });

    const data = await res.json();

    if (data.success === false) {
      comparisonResults.innerHTML = `<p class='error'>Error: ${data.message}</p>`;
    } else {
      displayComparisonResults(data);
      clearCompareBtn.classList.remove("hidden");
    }
  } catch (err) {
    comparisonResults.innerHTML = `<p class='error'>Failed to compare: ${err.message}</p>`;
  } finally {
    compareBtn.disabled = false;
    compareBtn.textContent = "Compare Databases";
  }
});

clearCompareBtn.addEventListener("click", () => {
  comparisonResults.innerHTML = "";
  comparisonResults.classList.add("hidden");
  clearCompareBtn.classList.add("hidden");
});

function displayComparisonResults(data) {
  const html = `
    <div class="comparison-summary">
      <h3>Database Comparison: ${escapeHtml(data.sourceDatabase)} vs ${escapeHtml(data.targetDatabase)}</h3>

      <div class="summary-stats">
        <div class="stat-card">
          <div class="stat-label">SQL Server Tables</div>
          <div class="stat-value">${data.sqlServerTableCount}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">PostgreSQL Tables</div>
          <div class="stat-value">${data.postgresTableCount}</div>
        </div>
        <div class="stat-card ${data.missingInPostgres.length > 0 ? 'stat-warning' : ''}">
          <div class="stat-label">Missing in PostgreSQL</div>
          <div class="stat-value">${data.missingInPostgres.length}</div>
        </div>
        <div class="stat-card ${data.extraInPostgres.length > 0 ? 'stat-info' : ''}">
          <div class="stat-label">Extra in PostgreSQL</div>
          <div class="stat-value">${data.extraInPostgres.length}</div>
        </div>
      </div>

      ${data.schemas.length > 0 ? `
        <div class="schema-summary">
          <h4>Schema Summary</h4>
          <table class="comparison-table">
            <thead>
              <tr>
                <th>Schema</th>
                <th>SQL Server Tables</th>
                <th>PostgreSQL Tables</th>
                <th>Match</th>
              </tr>
            </thead>
            <tbody>
              ${data.schemas.map(s => `
                <tr class="${s.sqlServerTables === s.postgresTables ? '' : 'mismatch'}">
                  <td><strong>${escapeHtml(s.schemaName)}</strong></td>
                  <td>${s.sqlServerTables}</td>
                  <td>${s.postgresTables}</td>
                  <td>${s.sqlServerTables === s.postgresTables ? '✓' : '✗'}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      ` : ''}

      ${data.missingInPostgres.length > 0 ? `
        <div class="missing-tables">
          <h4>⚠️ Missing in PostgreSQL (${data.missingInPostgres.length})</h4>
          <ul>
            ${data.missingInPostgres.map(t => `<li>${escapeHtml(t)}</li>`).join('')}
          </ul>
        </div>
      ` : ''}

      ${data.extraInPostgres.length > 0 ? `
        <div class="extra-tables">
          <h4>ℹ️ Extra in PostgreSQL (${data.extraInPostgres.length})</h4>
          <ul>
            ${data.extraInPostgres.map(t => `<li>${escapeHtml(t)}</li>`).join('')}
          </ul>
        </div>
      ` : ''}

      <div class="table-details">
        <h4>Row Count Comparison</h4>
        <table class="comparison-table">
          <thead>
            <tr>
              <th>Schema.Table</th>
              <th>SQL Server Rows</th>
              <th>PostgreSQL Rows</th>
              <th>Match</th>
            </tr>
          </thead>
          <tbody>
            ${data.tables.map(t => `
              <tr class="${!t.existsInSqlServer || !t.existsInPostgres ? 'missing' : (t.rowCountMatch ? '' : 'mismatch')}">
                <td><strong>${escapeHtml(t.schema)}.${escapeHtml(t.tableName)}</strong></td>
                <td>${t.existsInSqlServer ? t.sqlServerRows.toLocaleString() : '❌'}</td>
                <td>${t.existsInPostgres ? (t.postgresRows >= 0 ? t.postgresRows.toLocaleString() : 'Error') : '❌'}</td>
                <td>${!t.existsInSqlServer || !t.existsInPostgres ? '❌' : (t.rowCountMatch ? '✓' : '✗')}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </div>
  `;

  comparisonResults.innerHTML = html;
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}
