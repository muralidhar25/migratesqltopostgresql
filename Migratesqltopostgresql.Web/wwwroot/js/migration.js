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
        postgresAdminConnection: pgCs
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
