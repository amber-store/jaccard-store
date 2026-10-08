// The admin page of jaccard-store. It reads the server's JSON API and
// renders it; it changes nothing. Ref names and endpoint IDs come from
// whoever pushed, so every value reaches the page as text, never as markup.
"use strict";

const view = document.getElementById("view");
const PAGE = 100;

// h builds an element: h("td", {class: "num"}, "12"). Children are nodes,
// strings, or lists of them however deep; a string becomes a text node, and
// null and false are left out.
function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [name, value] of Object.entries(attrs || {})) {
    if (value === false || value == null) continue;
    if (name === "on") {
      for (const [event, fn] of Object.entries(value)) el.addEventListener(event, fn);
    } else {
      el.setAttribute(name, value === true ? "" : String(value));
    }
  }
  for (const child of children.flat(Infinity)) {
    if (child == null || child === false) continue;
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return el;
}

async function api(path) {
  const res = await fetch(path, { headers: { Accept: "application/json" } });
  let body = null;
  try {
    body = await res.json();
  } catch (_) {
    // An answer without a body is reported by its status below.
  }
  if (!res.ok) {
    throw new Error((body && body.error) || `the server answered ${res.status}`);
  }
  return body;
}

function bytes(n) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let f = n, u = 0;
  while (f >= 1024 && u < units.length - 1) { f /= 1024; u++; }
  return u === 0 ? `${n} B` : `${f.toFixed(2)} ${units[u]}`;
}

const count = (n) => Number(n).toLocaleString("en-US");
const percent = (f) => `${(100 * f).toFixed(1)} %`;
const when = (s) => new Date(s).toLocaleString();
const short = (hex) => `${hex.slice(0, 12)}…`;

function keyLink(root) {
  return h("a", { class: "key", href: `#/packs/${root}`, title: root }, short(root));
}

function kindBadge(kind) {
  return h("span", { class: `kind ${kind}` }, kind);
}

function tile(label, value, note, meter) {
  return h("div", { class: "tile" },
    h("div", { class: "label" }, label),
    h("div", { class: "value" }, value),
    note && h("div", { class: "note" }, note),
    meter != null && h("meter", { min: 0, max: 1, value: Math.max(0, Math.min(1, meter)) }));
}

function table(columns, rows, empty) {
  if (rows.length === 0) return h("p", { class: "notice" }, empty);
  return h("div", { class: "scroll" },
    h("table", null,
      h("thead", null, h("tr", null, columns.map((c) => h("th", { class: c.num && "num", scope: "col" }, c.title)))),
      h("tbody", null, rows.map((row) =>
        h("tr", null, columns.map((c) => h("td", { class: c.num ? "num" : c.class }, c.cell(row))))))));
}

// paged renders a list that grows by a page whenever "Load more" is pressed.
// load(after) fetches a page and returns {rows, more}; cursor(row) names the
// row the next page comes after.
function paged(columns, load, cursor, empty) {
  const holder = h("div");
  let rows = [];
  async function next() {
    const after = rows.length ? cursor(rows[rows.length - 1]) : "";
    const page = await load(after);
    rows = rows.concat(page.rows);
    // replaceChildren turns anything that is not a node into text, so the
    // button is left out, not passed as false.
    const nodes = [table(columns, rows, empty)];
    if (page.more) {
      nodes.push(h("button", { class: "more", type: "button", on: { click: () => next().catch(showError) } }, "Load more"));
    }
    holder.replaceChildren(...nodes);
  }
  return { holder, next };
}

async function overview() {
  const s = await api("api/stats");
  return [
    h("h1", null, "Overview"),
    h("div", { class: "tiles" },
      tile("References", count(s.refs)),
      tile("Base packs", count(s.base_packs)),
      tile("Patch packs", count(s.patch_packs)),
      tile("Open uploads", count(s.uploads)),
      tile("Queued deletions", count(s.deletions), "objects waiting to leave the bucket")),
    h("h2", null, "Size"),
    h("div", { class: "tiles" },
      tile("In the bucket", bytes(s.s3_bytes), "data, indexes and links"),
      tile("Stored objects", bytes(s.stored_bytes), "uncompressed, each pack counted once"),
      tile("Referenced", bytes(s.logical_bytes), "what the references hold, each taken alone")),
    h("h2", null, "Rates"),
    h("div", { class: "tiles" },
      tile("Deduplication", percent(s.dedup_rate), "of the referenced bytes are not stored twice", s.dedup_rate),
      tile("Compression", s.stored_bytes ? percent(s.compression) : "–", "compressed data over stored objects", s.compression)),
  ];
}

async function refs() {
  const filter = h("input", { type: "search", placeholder: "Names starting with…", "aria-label": "Prefix of the reference names" });
  const columns = [
    { title: "Name", class: "name", cell: (r) => r.name },
    { title: "Pack", cell: (r) => [kindBadge(r.kind), " ", keyLink(r.root)] },
    { title: "Objects", num: true, cell: (r) => count(r.ref_objects) },
    { title: "Size", num: true, cell: (r) => bytes(r.ref_bytes) },
    { title: "Deduplicated", num: true, cell: (r) => r.kind === "patch"
        ? [bytes(r.shared_bytes), h("div", { class: "sub" }, percent(r.dedup))] : "–" },
    { title: "Parent, out of reach", num: true, cell: (r) => r.parent_root
        ? [bytes(r.parent_unreachable_bytes), h("div", { class: "sub" }, `${count(r.parent_unreachable_objects)} objects`)] : "–" },
    { title: "Updated", cell: (r) => [when(r.updated_at), h("div", { class: "sub key", title: r.updated_by }, short(r.updated_by))] },
  ];
  const body = h("div");
  async function show() {
    const prefix = filter.value;
    const list = paged(columns, async (after) => {
      const q = new URLSearchParams({ prefix, after, limit: PAGE });
      const page = await api(`api/refs?${q}`);
      return { rows: page.refs, more: page.more };
    }, (r) => r.name, prefix ? "No reference has a name starting with that." : "The store holds no references yet.");
    await list.next();
    body.replaceChildren(list.holder);
  }
  let timer;
  filter.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(() => show().catch(showError), 250);
  });
  await show();
  return [h("h1", null, "References"), h("div", { class: "toolbar" }, filter), body];
}

async function packs() {
  const columns = [
    { title: "Root", cell: (p) => keyLink(p.root) },
    { title: "Kind", cell: (p) => kindBadge(p.kind) },
    { title: "Objects", num: true, cell: (p) => count(p.objects) },
    { title: "Stored", num: true, cell: (p) => bytes(p.bytes) },
    { title: "In the bucket", num: true, cell: (p) => bytes(p.data_size + p.index_size + p.links_size) },
    { title: "Refs", num: true, cell: (p) => count(p.refs) },
    { title: "Children", num: true, cell: (p) => count(p.children) },
    { title: "Uploaded by", cell: (p) => h("span", { class: "key", title: p.uploader }, short(p.uploader)) },
    { title: "Uploaded", cell: (p) => when(p.uploaded_at) },
  ];
  const list = paged(columns, async (after) => {
    const q = new URLSearchParams({ after, limit: PAGE });
    const page = await api(`api/packs?${q}`);
    return { rows: page.packs, more: page.more };
  }, (p) => p.id, "The store holds no packs yet.");
  await list.next();
  return [h("h1", null, "Packs"), list.holder];
}

function fact(term, ...value) {
  return [h("dt", null, term), h("dd", null, ...value)];
}

async function pack(root) {
  const d = await api(`api/packs/${encodeURIComponent(root)}`);
  const p = d.pack;
  const facts = [
    fact("Root", h("span", { class: "key full" }, p.root)),
    fact("Kind", kindBadge(p.kind)),
    fact("Uploaded by", h("span", { class: "key full" }, p.uploader)),
    fact("Uploaded", when(p.uploaded_at)),
    fact("Objects in the pack", `${count(p.objects)}, ${bytes(p.bytes)} uncompressed`),
    fact("In the bucket", `${bytes(p.data_size)} data, ${bytes(p.index_size)} index`,
      p.links_size ? `, ${bytes(p.links_size)} links` : ""),
  ];
  if (d.parent) {
    const refBytes = p.bytes + p.shared_bytes;
    facts.push(
      fact("Parent", keyLink(d.parent.root)),
      fact("Held by the parent", `${count(p.shared_objects)} objects, ${bytes(p.shared_bytes)}`,
        refBytes ? ` (${percent(p.shared_bytes / refBytes)} of the reference)` : ""),
      fact("Parent, out of reach", `${count(d.parent.objects - p.shared_objects)} objects, ${bytes(d.parent.bytes - p.shared_bytes)}`));
  }
  return [
    h("h1", null, "Pack"),
    h("dl", { class: "facts" }, facts),
    h("h2", null, `References (${d.refs.length})`),
    d.refs.length
      ? h("ul", { class: "plain" }, d.refs.map((name) => h("li", null, name)))
      : h("p", { class: "notice" }, "No reference points at this pack. It is kept for the packs that lean on it."),
    h("h2", null, `Patch packs leaning on it (${d.children.length})`),
    d.children.length
      ? h("ul", { class: "plain" }, d.children.map((c) => h("li", null, keyLink(c))))
      : h("p", { class: "notice" }, "None."),
  ];
}

async function uploads() {
  const data = await api("api/uploads");
  const columns = [
    { title: "Reference", class: "name", cell: (u) => u.name },
    { title: "State", cell: (u) => kindBadge(u.state) },
    { title: "Root", cell: (u) => h("span", { class: "key", title: u.root }, short(u.root)) },
    { title: "Parent", cell: (u) => (u.parent_root ? keyLink(u.parent_root) : "–") },
    { title: "Data", num: true, cell: (u) => [bytes(u.data_size), u.multipart && h("div", { class: "sub" }, "in parts")] },
    { title: "Objects", num: true, cell: (u) => count(u.objects) },
    { title: "By", cell: (u) => h("span", { class: "key", title: u.uploader }, short(u.uploader)) },
    { title: "Opened", cell: (u) => when(u.issued_at) },
    { title: "Expires", cell: (u) => when(u.deadline) },
  ];
  return [h("h1", null, "Open uploads"), table(columns, data.uploads, "No upload is open.")];
}

function showError(err) {
  view.replaceChildren(h("p", { class: "error", role: "alert" }, `Could not read the store: ${err.message}`));
}

// route maps the fragment to a view and the tab it belongs to.
function route() {
  const parts = location.hash.replace(/^#\/?/, "").split("/").filter(Boolean);
  if (parts[0] === "refs") return ["refs", refs];
  if (parts[0] === "packs" && parts[1]) return ["packs", () => pack(parts[1])];
  if (parts[0] === "packs") return ["packs", packs];
  if (parts[0] === "uploads") return ["uploads", uploads];
  return ["overview", overview];
}

let showing = 0;

async function show() {
  const [tab, render] = route();
  for (const a of document.querySelectorAll("nav a")) {
    const current = a.dataset.view === tab;
    a.classList.toggle("current", current);
    if (current) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current");
  }
  // A slow answer must not overwrite the view the reader moved on to.
  const turn = ++showing;
  try {
    const nodes = await render();
    if (turn === showing) view.replaceChildren(...nodes.flat(Infinity));
  } catch (err) {
    if (turn === showing) showError(err);
  }
}

window.addEventListener("hashchange", show);
document.getElementById("refresh").addEventListener("click", show);
show();
