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

// saved says what one size of the chain is against the one before it: how
// much less, and what part of the one before that is. A step can also cost,
// when what it adds is more than what it takes away.
function saved(before, after) {
  if (!before) return "–";
  const less = before - after;
  if (less >= 0) return [bytes(less), " less", h("div", { class: "sub" }, percent(less / before))];
  return h("span", { class: "cost" }, bytes(-less), " more", h("div", { class: "sub" }, percent(-less / before)));
}

// link is one size of the chain: what it is, the size, a bar of it against
// the largest, and what the step to it saved.
function link(most, { title, note, size, against, part, total }) {
  return h("tr", { class: part ? "part" : total && "total" },
    h("th", { scope: "row" }, title, note && h("div", { class: "sub" }, note)),
    h("td", { class: "num" }, bytes(size)),
    h("td", { class: "bar" }, !part && h("meter", { min: 0, max: Math.max(most, 1), value: size })),
    h("td", { class: "num" }, against));
}

// plural counts things in words: 1 pack, 2 packs.
const plural = (n, one, many) => `${count(n)} ${n === 1 ? one : many}`;

// topColumns are the columns the two lists of packs on the overview share,
// around the one count each is ordered by.
function topColumns(first, second) {
  return [
    { title: "Root", cell: (p) => keyLink(p.root) },
    { title: "Kind", cell: (p) => kindBadge(p.kind) },
    first,
    second,
    { title: "Unpacked", num: true, cell: (p) => bytes(p.unpacked_bytes) },
    { title: "In the pack", num: true, cell: (p) => [bytes(p.bytes), h("div", { class: "sub" }, plural(p.objects, "object", "objects"))] },
    { title: "In the bucket", num: true, cell: (p) => bytes(p.data_size + p.index_size + p.links_size) },
  ];
}

async function overview() {
  const [s, top] = await Promise.all([api("api/stats"), api("api/top?limit=20")]);
  const most = Math.max(s.unpacked_bytes, s.object_bytes, s.pack_bytes, s.s3_bytes);
  const waiting = [
    s.deletions > 0 && `${plural(s.deletions, "object", "objects")} queued for deletion`,
    s.uploads > 0 && plural(s.uploads, "open upload", "open uploads"),
  ].filter(Boolean);
  return [
    h("h1", null, "Overview"),
    h("h2", null, "From unpacked to the bucket"),
    h("p", { class: "lede" },
      "What the references of this store come to. Each line is the one above it after one more thing the store does to save room."),
    h("div", { class: "scroll" },
      h("table", { class: "chain" },
        h("thead", null, h("tr", null,
          h("th", { scope: "col" }, "The references, as"),
          h("th", { class: "num", scope: "col" }, "Size"),
          h("th", { scope: "col" }),
          h("th", { class: "num", scope: "col" }, "Against the line above"))),
        h("tbody", null,
          link(most, {
            title: "Unpacked",
            note: "every reference in a directory of its own: its files and its directories",
            size: s.unpacked_bytes, against: "–",
          }),
          link(most, {
            title: "One pack for each reference",
            note: "content addressing: within a reference every object is there once",
            size: s.object_bytes, against: saved(s.unpacked_bytes, s.object_bytes),
          }),
          link(most, {
            title: "The packs there are",
            note: "sharing: a patch pack leans on a base pack, and references of the same content are one pack",
            size: s.pack_bytes, against: saved(s.object_bytes, s.pack_bytes),
          }),
          link(most, {
            part: true,
            title: "in packs that references point at",
            note: "what sharing saves",
            size: s.referenced_pack_bytes, against: saved(s.object_bytes, s.referenced_pack_bytes),
          }),
          link(most, {
            part: true,
            title: s.unreferenced_packs
              ? `in ${plural(s.unreferenced_packs, "pack", "packs")} no reference points at`
              : "in packs no reference points at: there is none",
            note: "what sharing costs: such a pack is kept whole for the patch packs that lean on it",
            size: s.unreferenced_pack_bytes,
            against: s.unreferenced_pack_bytes ? [bytes(s.unreferenced_data_bytes), h("div", { class: "sub" }, "of the bucket")] : "–",
          }),
          link(most, {
            title: "Pack data in the bucket",
            note: "compression",
            size: s.data_bytes, against: saved(s.pack_bytes, s.data_bytes),
          }),
          link(most, {
            part: true,
            title: "indexes and links beside it",
            size: s.index_bytes, against: "–",
          }),
          link(most, {
            total: true,
            title: "In the bucket",
            size: s.s3_bytes,
            against: s.unpacked_bytes ? [percent(s.s3_bytes / s.unpacked_bytes), h("div", { class: "sub" }, "of unpacked")] : "–",
          })))),
    h("p", { class: "footnote" },
      "Sizes are of objects as they are, before compression, down to the packs there are. ",
      "Unpacked is the size the root key of a reference records for its tree; the server takes the key's word for it. ",
      waiting.length > 0 && `Not counted: ${waiting.join(" and ")}, which are in the bucket as well.`),
    h("h2", null, "Counts"),
    h("div", { class: "tiles" },
      tile("References", count(s.refs)),
      tile("Base packs", count(s.base_packs), "each holds all of one root"),
      tile("Patch packs", count(s.patch_packs), "each holds what its parent lacks"),
      tile("Packs without a reference", count(s.unreferenced_packs), "kept for the patch packs that lean on them"),
      tile("Open uploads", count(s.uploads)),
      tile("Queued deletions", count(s.deletions), "objects waiting to leave the bucket")),
    h("h2", null, "Packs the most references point at"),
    table(topColumns(
      { title: "References", num: true, cell: (p) => count(p.refs) },
      { title: "Patch packs on it", num: true, cell: (p) => count(p.children) },
    ), top.by_refs, "The store holds no references yet."),
    h("h2", null, "Packs the most patch packs lean on"),
    table(topColumns(
      { title: "Patch packs on it", num: true, cell: (p) => count(p.children) },
      { title: "References", num: true, cell: (p) => count(p.refs) },
    ).concat([
      { title: "The most one of them uses", num: true, cell: (p) =>
          [bytes(p.largest_share), h("div", { class: "sub" }, p.bytes ? `${percent(p.largest_share / p.bytes)} of the pack` : "")] },
    ]), top.by_children, "No pack is leaned on: every reference has a base pack of its own."),
  ];
}

async function refs() {
  const filter = h("input", { type: "search", placeholder: "Names starting with…", "aria-label": "Prefix of the reference names" });
  const columns = [
    { title: "Name", class: "name", cell: (r) => r.name },
    { title: "Pack", cell: (r) => [kindBadge(r.kind), " ", keyLink(r.root)] },
    { title: "Unpacked", num: true, cell: (r) => bytes(r.unpacked_bytes) },
    { title: "As objects", num: true, cell: (r) =>
        [bytes(r.ref_bytes), h("div", { class: "sub" }, plural(r.ref_objects, "object", "objects"))] },
    { title: "In its own pack", num: true, cell: (r) =>
        [bytes(r.pack_bytes), h("div", { class: "sub" }, `${bytes(r.pack_data_size)} in the bucket`)] },
    { title: "From its parent", num: true, cell: (r) => r.parent_root
        ? [bytes(r.shared_bytes), h("div", { class: "sub" }, r.ref_bytes ? `${percent(r.shared_bytes / r.ref_bytes)} of the reference` : "")] : "–" },
    { title: "Of the parent, unused", num: true, cell: (r) => {
        if (!r.parent_root) return "–";
        const parent = r.shared_bytes + r.parent_unreachable_bytes;
        return [bytes(r.parent_unreachable_bytes), h("div", { class: "sub" }, parent ? `${percent(r.parent_unreachable_bytes / parent)} of the parent` : "")];
      } },
    { title: "Updated", cell: (r) => [when(r.updated_at), h("div", { class: "sub key", title: r.updated_by }, short(r.updated_by))] },
  ];
  const body = h("div");
  let asked = 0;
  async function show() {
    const prefix = filter.value;
    // The answer to an earlier prefix can come after the answer to a later
    // one; only the last one asked for is shown.
    const turn = ++asked;
    const list = paged(columns, async (after) => {
      const q = new URLSearchParams({ prefix, after, limit: PAGE });
      const page = await api(`api/refs?${q}`);
      return { rows: page.refs, more: page.more };
    }, (r) => r.name, prefix ? "No reference has a name starting with that." : "The store holds no references yet.");
    await list.next();
    if (turn === asked) body.replaceChildren(list.holder);
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
    { title: "Unpacked", num: true, cell: (p) => bytes(p.unpacked_bytes) },
    { title: "In the pack", num: true, cell: (p) => [bytes(p.bytes), h("div", { class: "sub" }, plural(p.objects, "object", "objects"))] },
    { title: "In the bucket", num: true, cell: (p) => bytes(p.data_size + p.index_size + p.links_size) },
    { title: "References", num: true, cell: (p) => p.refs ? count(p.refs) : h("span", { class: "cost" }, "none") },
    { title: "Patch packs on it", num: true, cell: (p) => count(p.children) },
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
    fact("Unpacked", bytes(p.unpacked_bytes), " is what the tree of this root comes to, by its key"),
    fact("In the pack", `${plural(p.objects, "object", "objects")}, ${bytes(p.bytes)} before compression`),
    fact("In the bucket", `${bytes(p.data_size)} data, ${bytes(p.index_size)} index`,
      p.links_size ? `, ${bytes(p.links_size)} links` : ""),
  ];
  if (d.parent) {
    const refBytes = p.bytes + p.shared_bytes;
    const unused = d.parent.bytes - p.shared_bytes;
    facts.push(
      fact("Parent", keyLink(d.parent.root)),
      fact("From the parent", `${plural(p.shared_objects, "object", "objects")}, ${bytes(p.shared_bytes)}`,
        refBytes ? ` (${percent(p.shared_bytes / refBytes)} of the reference)` : ""),
      fact("Of the parent, unused", `${plural(d.parent.objects - p.shared_objects, "object", "objects")}, ${bytes(unused)}`,
        d.parent.bytes ? ` (${percent(unused / d.parent.bytes)} of the parent)` : ""));
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
