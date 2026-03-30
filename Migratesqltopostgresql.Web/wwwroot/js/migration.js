const dbNameInput = document.getElementById("dbname");
const targetInput = document.getElementById("targetDbname");
const migrateBtn = document.getElementById("migrateBtn");
const progressBar = document.getElementById("progressBar");
const progressText = document.getElementById("progressText");
const statusText = document.getElementById("statusText");
const errorText = document.getElementById("errorText");
const logBox = document.getElementById("logBox");
const connModal = document.getElementById("connModal");
const connModalError = document.getElementById("connModalError");
const stepSqlServer = document.getElementById("stepSqlServer");
const stepPostgres = document.getElementById("stepPostgres");
const sqlServerTemplateInput = document.getElementById("sqlServerTemplate");
const postgresAdminConnectionInput = document.getElementById("postgresAdminConnection");
const connCancelBtn = document.getElementById("connCancelBtn");
const connBackBtn = document.getElementById("connBackBtn");
const connNextBtn = document.getElementById("connNextBtn");
const connStartBtn = document.getElementById("connStartBtn");

let pollHandle = null;
let modalStep = 1;

function setProgress(value) {
  const normalized = Math.max(0, Math.min(100, Number(value) || 0));
  progressBar.style.width = `${normalized}%`;
  progressText.textContent = `${normalized}%`;
}

function setRunning(isRunning) {
  migrateBtn.disabled = isRunning;
  dbNameInput.disabled = isRunning;
  targetInput.disabled = isRunning;
}

function showLogs(logs) {
  if (!Array.isArray(logs)) {
    return;
  }

  logBox.textContent = logs.join("\n");
  logBox.scrollTop = logBox.scrollHeight;
}

async function fetchStatus(jobId) {
  const response = await fetch(`/api/migration/status/${jobId}`);
  if (!response.ok) {
    throw new Error("Unable to fetch migration status.");
  }

  return response.json();
}

function beginPolling(jobId) {
  if (pollHandle) {
    clearInterval(pollHandle);
  }

  pollHandle = setInterval(async () => {
    try {
      const data = await fetchStatus(jobId);
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

async function startMigration() {
  const dbName = dbNameInput.value.trim();
  const targetDbName = targetInput.value.trim();

  if (!dbName) {
    errorText.textContent = "Please enter source DB name.";
    return;
  }

  openConnModal();
}

function openConnModal() {
  modalStep = 1;
  connModalError.textContent = "";
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

function toPostgresStep() {
  const sqlTemplate = sqlServerTemplateInput.value.trim();
  if (!sqlTemplate) {
    connModalError.textContent = "Please enter SQL Server connection template.";
    return;
  }

  if (!sqlTemplate.toLowerCase().includes("{dbname}")) {
    connModalError.textContent = "SQL Server template must include {dbname}.";
    return;
  }

  modalStep = 2;
  connModalError.textContent = "";
  stepSqlServer.classList.add("hidden");
  stepPostgres.classList.remove("hidden");
  connBackBtn.classList.remove("hidden");
  connNextBtn.classList.add("hidden");
  connStartBtn.classList.remove("hidden");
  postgresAdminConnectionInput.focus();
}

function toSqlServerStep() {
  modalStep = 1;
  connModalError.textContent = "";
  stepSqlServer.classList.remove("hidden");
  stepPostgres.classList.add("hidden");
  connBackBtn.classList.add("hidden");
  connNextBtn.classList.remove("hidden");
  connStartBtn.classList.add("hidden");
}

async function submitMigration() {
  const dbName = dbNameInput.value.trim();
  const targetDbName = targetInput.value.trim();
  const sqlServerConnectionTemplate = sqlServerTemplateInput.value.trim();
  const postgresAdminConnection = postgresAdminConnectionInput.value.trim();

  if (!postgresAdminConnection) {
    connModalError.textContent = "Please enter PostgreSQL admin connection string.";
    return;
  }

  closeConnModal();
  setRunning(true);
  setProgress(0);
  statusText.textContent = "Submitting migration job...";
  errorText.textContent = "";
  logBox.textContent = "";

  try {
    const response = await fetch("/api/migration/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dbName,
        targetDbName: targetDbName || null,
        sqlServerConnectionTemplate,
        postgresAdminConnection
      })
    });

    if (!response.ok) {
      const errorBody = await response.json().catch(() => ({ message: "Failed to start migration." }));
      throw new Error(errorBody.message || "Failed to start migration.");
    }

    const body = await response.json();
    beginPolling(body.jobId);
  } catch (err) {
    setRunning(false);
    statusText.textContent = "Failed to start";
    errorText.textContent = err?.message || "Failed to start migration.";
  }
}

migrateBtn.addEventListener("click", startMigration);
connCancelBtn.addEventListener("click", closeConnModal);
connNextBtn.addEventListener("click", toPostgresStep);
connBackBtn.addEventListener("click", toSqlServerStep);
connStartBtn.addEventListener("click", submitMigration);

connModal.addEventListener("click", (event) => {
  if (event.target === connModal) {
    closeConnModal();
  }
});
