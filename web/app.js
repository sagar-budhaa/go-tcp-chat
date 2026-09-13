"use strict";

const $ = (id) => document.getElementById(id);
let ws = null;
let selfName = "";
let selfRoom = "";
const members = new Set();

function line(text, cls) {
  const div = document.createElement("div");
  if (cls) div.className = cls;
  div.textContent = text;
  $("msgs").appendChild(div);
  $("msgs").scrollTop = $("msgs").scrollHeight;
}

function renderMembers() {
  const ul = $("members");
  ul.innerHTML = "";
  [...members].sort().forEach((n) => {
    const li = document.createElement("li");
    li.textContent = n;
    ul.appendChild(li);
  });
}

function show(m) {
  switch (m.type) {
    case "msg":
      line(`[${m.from}] ${m.body}`);
      break;
    case "dm":
      line(`[dm ${m.from} -> ${m.to || "?"}] ${m.body}`, "dm");
      break;
    case "join":
      line(`*** ${m.from} joined #${m.room} ***`, "sys");
      members.add(m.from);
      renderMembers();
      break;
    case "leave":
      line(`*** ${m.from} left #${m.room} ***`, "sys");
      members.delete(m.from);
      renderMembers();
      break;
    case "error":
      line(`server error: ${m.body}`, "err");
      break;
  }
}

$("join").onclick = () => {
  selfName = $("name").value.trim();
  selfRoom = $("room").value.trim() || "general";
  if (!selfName) return;
  $("login").classList.add("hidden");
  $("chat").classList.remove("hidden");
  $("title").textContent = `#${selfRoom} as ${selfName}`;
  members.clear();
  renderMembers();

  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(
    `${proto}://${location.host}/ws?name=${encodeURIComponent(selfName)}&room=${encodeURIComponent(selfRoom)}`
  );
  ws.onopen = () => ($("status").textContent = "connected");
  ws.onclose = () => {
    $("status").textContent = "disconnected";
    line("disconnected — rejoin to reconnect", "err");
  };
  ws.onmessage = (ev) => {
    try {
      show(JSON.parse(ev.data));
    } catch {
      line("bad frame from server", "err");
    }
  };
};

$("leave").onclick = () => {
  if (ws) ws.close();
  $("chat").classList.add("hidden");
  $("login").classList.remove("hidden");
};

$("msgform").onsubmit = (e) => {
  e.preventDefault();
  const text = $("msgtext").value;
  if (!text || !ws) return;
  if (text === "/quit") {
    ws.close();
    return;
  }
  if (text.startsWith("/dm ")) {
    const rest = text.slice(4).split(" ");
    const to = rest.shift();
    const body = rest.join(" ");
    if (to && body) ws.send(JSON.stringify({ v: 1, type: "dm", to, body }));
  } else {
    ws.send(JSON.stringify({ v: 1, type: "msg", body: text }));
  }
  $("msgtext").value = "";
};

$("dmform").onsubmit = (e) => {
  e.preventDefault();
  if (!ws) return;
  const to = $("dmto").value.trim();
  const body = $("dmtext").value;
  if (!to || !body) return;
  ws.send(JSON.stringify({ v: 1, type: "dm", to, body }));
  $("dmtext").value = "";
};
