/* Minimal UI for manual testing of the Quiz Service API. */

const TOKEN_KEY = "quiz_ui_access_token";

function $(id) {
  return document.getElementById(id);
}

function log(line) {
  const el = $("log");
  const ts = new Date().toISOString().replace("T", " ").replace("Z", "Z");
  el.textContent += `[${ts}] ${line}\n`;
  el.scrollTop = el.scrollHeight;
}

function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

function setToken(token) {
  if (!token) localStorage.removeItem(TOKEN_KEY);
  else localStorage.setItem(TOKEN_KEY, token);
  updateAuthUI();
}

async function api(path, { method = "GET", body = null } = {}) {
  const headers = { "Content-Type": "application/json" };
  const token = getToken();
  if (token) headers.Authorization = `Bearer ${token}`;

  const res = await fetch(path, {
    method,
    headers,
    body: body ? JSON.stringify(body) : null,
  });

  const text = await res.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  if (!res.ok) {
    const msg = typeof data === "string" ? data : JSON.stringify(data);
    throw new Error(`${method} ${path} -> ${res.status}: ${msg}`);
  }
  return data;
}

function updateAuthUI() {
  const token = getToken();
  $("logoutBtn").disabled = !token;
  if (!token) {
    $("whoamiPill").textContent = "Not authenticated";
    $("whoamiPill").classList.remove("ok");
  }
}

function setWhoami(user) {
  if (!user) {
    $("whoamiPill").textContent = "Not authenticated";
    return;
  }
  $("whoamiPill").textContent = `${user.username} (${user.role}) id=${user.id}` + (user.team_id ? ` team=${user.team_id}` : "");
}

function tabInit() {
  const tabs = document.querySelectorAll(".tab");
  for (const t of tabs) {
    t.addEventListener("click", () => {
      for (const x of tabs) x.classList.remove("active");
      t.classList.add("active");
      const name = t.dataset.tab;
      for (const pane of document.querySelectorAll(".tabpane")) pane.classList.add("hidden");
      $(`tab-${name}`).classList.remove("hidden");
    });
  }
}

function renderList(el, items, renderItem) {
  if (!items || items.length === 0) {
    el.innerHTML = `<div class="item"><div class="muted">Empty</div></div>`;
    return;
  }
  el.innerHTML = "";
  for (const it of items) {
    const row = document.createElement("div");
    row.className = "item";
    row.appendChild(renderItem(it));
    el.appendChild(row);
  }
}

function renderPickList(el, items, labelFn, onChange) {
  if (!items || items.length === 0) {
    el.innerHTML = `<div class="item"><div class="muted">Empty</div></div>`;
    return;
  }
  el.innerHTML = "";
  for (const it of items) {
    const row = document.createElement("div");
    row.className = "item";
    const wrap = document.createElement("div");
    wrap.className = "pick";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.dataset.id = String(it.id);
    cb.addEventListener("change", onChange);
    const txt = document.createElement("div");
    txt.innerHTML = `<div>${labelFn(it)}</div><div class="muted mono">id=${it.id}</div>`;
    wrap.appendChild(cb);
    wrap.appendChild(txt);
    row.appendChild(wrap);
    el.appendChild(row);
  }
}

async function refreshTeams() {
  const teams = await api("/teams");
  renderList($("teamsList"), teams, (t) => {
    const d = document.createElement("div");
    d.innerHTML = `<div><b>${t.name}</b></div><div class="muted mono">id=${t.id} points=${t.total_points} W/L=${t.wins}/${t.losses}</div>`;
    return d;
  });
  renderPickList($("teamsPick"), teams, (t) => t.name, () => {});
  log(`Loaded teams: ${teams.length}`);
}

async function refreshQuestions() {
  const qs = await api("/questions");
  renderList($("questionsList"), qs, (q) => {
    const d = document.createElement("div");
    const opts = (q.options || []).map((o) => `${o.id}:${o.text}`).join(" | ");
    d.innerHTML = `<div><b>${q.text}</b></div><div class="muted mono">${opts}</div><div class="muted mono">id=${q.id}</div>`;
    return d;
  });
  renderPickList($("questionsPick"), qs, (q) => q.text, () => {});
  log(`Loaded questions: ${qs.length}`);
}

async function refreshGames() {
  const games = await api("/games");
  renderList($("gamesList"), games, (g) => {
    const wrap = document.createElement("div");
    wrap.style.display = "flex";
    wrap.style.alignItems = "center";
    wrap.style.justifyContent = "space-between";
    wrap.style.gap = "10px";

    const left = document.createElement("div");
    left.innerHTML = `<div><b>Game #${g.id}</b> <span class="muted">(${g.status})</span></div><div class="muted mono">scheduled_at=${g.scheduled_at}</div>`;

    const right = document.createElement("div");
    right.className = "row";
    right.style.gap = "8px";

    const startBtn = document.createElement("button");
    startBtn.className = "btn primary";
    startBtn.textContent = "Start";
    startBtn.addEventListener("click", async () => {
      try {
        const r = await api(`/games/${g.id}/start`, { method: "POST" });
        log(`Game started: ${JSON.stringify(r)}`);
      } catch (e) {
        log(String(e.message || e));
      }
    });

    const nextBtn = document.createElement("button");
    nextBtn.className = "btn";
    nextBtn.textContent = "Next";
    nextBtn.addEventListener("click", async () => {
      try {
        const r = await api(`/games/${g.id}/next-question`, { method: "POST" });
        log(`Next question: ${JSON.stringify(r)}`);
      } catch (e) {
        log(String(e.message || e));
      }
    });

    const useBtn = document.createElement("button");
    useBtn.className = "btn";
    useBtn.textContent = "Use in Player";
    useBtn.addEventListener("click", () => {
      $("playerGameId").value = String(g.id);
      log(`Player game_id set to ${g.id}`);
    });

    right.appendChild(useBtn);
    right.appendChild(startBtn);
    right.appendChild(nextBtn);
    wrap.appendChild(left);
    wrap.appendChild(right);
    return wrap;
  });
  log(`Loaded games: ${games.length}`);
}

function getPickedIds(containerId) {
  const el = $(containerId);
  const ids = [];
  for (const cb of el.querySelectorAll("input[type=checkbox]")) {
    if (cb.checked) ids.push(Number(cb.dataset.id));
  }
  return ids;
}

async function createTeam() {
  const name = $("teamName").value.trim();
  if (!name) return;
  const t = await api("/teams", { method: "POST", body: { name } });
  log(`Created team: id=${t.id} name=${t.name}`);
  $("teamName").value = "";
  await refreshTeams();
}

async function createQuestion() {
  const text = $("questionText").value.trim();
  const correctIndex = Number($("correctIndex").value || "0");
  const opts = [];
  for (const inp of document.querySelectorAll("input.opt")) {
    const v = inp.value.trim();
    if (v) opts.push(v);
  }
  if (!text || opts.length < 2) {
    log("Question requires text and at least 2 options");
    return;
  }
  const q = await api("/questions", {
    method: "POST",
    body: { text, options: opts, correct_option_index: correctIndex },
  });
  log(`Created question: id=${q.id}`);
  $("questionText").value = "";
  await refreshQuestions();
}

async function createGame() {
  const teamIds = getPickedIds("teamsPick");
  const questionIds = getPickedIds("questionsPick");
  const scheduledAt = $("scheduledAt").value.trim() || new Date().toISOString();
  if (teamIds.length === 0 || questionIds.length === 0) {
    log("Pick at least 1 team and 1 question");
    return;
  }
  const g = await api("/games", {
    method: "POST",
    body: { scheduled_at: scheduledAt, team_ids: teamIds, question_ids: questionIds },
  });
  log(`Created game: id=${g.id}`);
  await refreshGames();
}

let ws = null;

function wsDisconnect() {
  if (ws) {
    try { ws.close(); } catch {}
  }
  ws = null;
}

function wsConnect(gameId) {
  wsDisconnect();
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const url = `${proto}://${location.host}/ws/games/${gameId}`;
  ws = new WebSocket(url);
  ws.onopen = () => {
    log(`WS connected: ${url}`);
  };
  ws.onmessage = (ev) => {
    const line = String(ev.data);
    const el = document.createElement("div");
    el.className = "item";
    el.innerHTML = `<div class="mono">${escapeHtml(line)}</div>`;
    $("realtimeLog").prepend(el);
    // Auto-refresh the current card on question event.
    try {
      const msg = JSON.parse(line);
      if (msg && msg.type === "question") loadCurrentQuestion();
    } catch {}
  };
  ws.onclose = () => log("WS closed");
  ws.onerror = () => log("WS error");
}

function escapeHtml(s) {
  return s.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

async function loadCurrentQuestion() {
  const gameId = Number($("playerGameId").value);
  if (!gameId) {
    log("Set game_id");
    return;
  }
  const data = await api(`/games/${gameId}/current-question`);
  renderCurrent(data);
  log(`Loaded current-question for game ${gameId}: status=${data.status} qid=${data.current_question_id}`);
}

function renderCurrent(data) {
  const card = $("currentCard");
  if (!data) {
    card.innerHTML = `<div class="muted">No data</div>`;
    return;
  }
  const q = data.question;
  const timeLeft = Math.max(0, Number(data.time_left_ms || 0));
  const header = `
    <div class="row" style="justify-content: space-between; align-items: center;">
      <div>
        <div><b>Game #${data.game_id}</b> <span class="muted">(${data.status})</span></div>
        <div class="muted mono">qid=${data.current_question_id ?? "null"} round=${data.round_seconds}s left=${Math.ceil(timeLeft/1000)}s</div>
      </div>
      <button class="btn" id="refreshCurrentBtn">Refresh</button>
    </div>
  `;
  if (!q) {
    card.innerHTML = header + `<div class="muted" style="margin-top:10px;">No active question.</div>`;
    $("refreshCurrentBtn").addEventListener("click", loadCurrentQuestion);
    return;
  }
  const opts = (q.options || []).map((o) => {
    return `<button class="optbtn" data-opt="${o.id}">${escapeHtml(o.text)} <span class="muted mono">(id=${o.id})</span></button>`;
  }).join("");

  card.innerHTML = header + `
    <div style="margin-top:10px;">
      <div><b>${escapeHtml(q.text)}</b></div>
      <div class="options">${opts}</div>
    </div>
  `;
  $("refreshCurrentBtn").addEventListener("click", loadCurrentQuestion);

  for (const btn of card.querySelectorAll(".optbtn")) {
    btn.addEventListener("click", async () => {
      const teamId = Number($("playerTeamId").value);
      const optId = Number(btn.dataset.opt);
      if (!teamId) {
        log("Set team_id (player)");
        return;
      }
      const gameId = Number($("playerGameId").value);
      try {
        const r = await api(`/games/${gameId}/answer`, { method: "POST", body: { team_id: teamId, option_id: optId } });
        log(`Answer result: ${JSON.stringify(r)}`);
        btn.classList.add(r.is_correct ? "correct" : "wrong");
      } catch (e) {
        log(String(e.message || e));
      }
    });
  }
}

async function doLogin() {
  const username = $("loginUsername").value.trim();
  const password = $("loginPassword").value;
  const data = await api("/auth/login", { method: "POST", body: { username, password } });
  setToken(data.access_token);
  log(`Logged in as ${username}`);
  await doMe();
}

async function doMe() {
  try {
    const me = await api("/auth/me");
    setWhoami(me);
    log(`me: ${JSON.stringify(me)}`);
  } catch (e) {
    setWhoami(null);
    log(String(e.message || e));
  }
}

function init() {
  tabInit();
  updateAuthUI();

  $("loginBtn").addEventListener("click", async () => {
    try { await doLogin(); } catch (e) { log(String(e.message || e)); }
  });
  $("meBtn").addEventListener("click", async () => {
    try { await doMe(); } catch (e) { log(String(e.message || e)); }
  });
  $("logoutBtn").addEventListener("click", () => {
    wsDisconnect();
    setToken(null);
    setWhoami(null);
    log("Logged out (token cleared)");
  });

  $("createTeamBtn").addEventListener("click", async () => {
    try { await createTeam(); } catch (e) { log(String(e.message || e)); }
  });
  $("refreshTeamsBtn").addEventListener("click", async () => {
    try { await refreshTeams(); } catch (e) { log(String(e.message || e)); }
  });

  $("createQuestionBtn").addEventListener("click", async () => {
    try { await createQuestion(); } catch (e) { log(String(e.message || e)); }
  });
  $("refreshQuestionsBtn").addEventListener("click", async () => {
    try { await refreshQuestions(); } catch (e) { log(String(e.message || e)); }
  });

  $("refreshGamesBtn").addEventListener("click", async () => {
    try { await refreshGames(); } catch (e) { log(String(e.message || e)); }
  });
  $("createGameBtn").addEventListener("click", async () => {
    try { await createGame(); } catch (e) { log(String(e.message || e)); }
  });

  $("connectWsBtn").addEventListener("click", () => {
    const gameId = Number($("playerGameId").value);
    if (!gameId) { log("Set game_id"); return; }
    wsConnect(gameId);
  });
  $("loadCurrentBtn").addEventListener("click", async () => {
    try { await loadCurrentQuestion(); } catch (e) { log(String(e.message || e)); }
  });

  // Convenience: try to preload data after /me succeeds.
  doMe().then(async () => {
    // If token exists, try refreshing admin lists (may fail for player role).
    if (getToken()) {
      try { await refreshTeams(); } catch {}
      try { await refreshQuestions(); } catch {}
      try { await refreshGames(); } catch {}
    }
  });
}

init();

