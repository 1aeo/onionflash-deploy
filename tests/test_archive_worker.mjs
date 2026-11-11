import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mock, test } from "node:test";
import worker from "../worker.mjs";

const hash = value => createHash("sha256").update(value).digest("hex");
const origin = "https://public.example/onionflash/public/";
const types = {
  "news/a/index.html": "text/html; charset=utf-8",
  "news/a/story.json": "application/json; charset=utf-8",
  "page/2/index.html": "text/html; charset=utf-8",
  "feed.xml": "application/xml; charset=utf-8",
  "tags/security.xml": "application/xml; charset=utf-8",
  "tags/bridges.xml": "application/xml; charset=utf-8",
  "topics/security/index.html": "text/html; charset=utf-8",
  "topics/security/page/2/index.html": "text/html; charset=utf-8",
  "topics/bridges/index.html": "text/html; charset=utf-8",
  "topics/bridges/page/2/index.html": "text/html; charset=utf-8",
  "feed.json": "application/feed+json; charset=utf-8",
};

function fixture(override = {}, contents = {}, recorded = {}) {
  const objects = new Map();
  const shards = {};
  const entries = {};
  for (const [key, content_type] of Object.entries(types)) {
    const body = new TextEncoder().encode(contents[key] ?? `public content for ${key}`);
    const sha256 = hash(body);
    entries[key] = { sha256, size: body.length, content_type: recorded[key] ?? content_type };
    objects.set(`${origin}objects/${sha256}`, { body, sha256 });
    const prefix = hash(key).slice(0, 2);
    shards[prefix] ??= { schema: 1, entries: {} };
    shards[prefix].entries[key] = entries[key];
  }
  const root = { schema: 1, shard_chars: 2, shards: {} };
  for (const [prefix, value] of Object.entries(shards)) {
    const body = new TextEncoder().encode(JSON.stringify(value));
    const sha = hash(body);
    objects.set(`${origin}manifests/${sha}.json`, { body });
    root.shards[prefix] = sha;
  }
  const body = new TextEncoder().encode(JSON.stringify(root));
  const rootHash = hash(body);
  objects.set(`${origin}manifests/${rootHash}.json`, { body });
  const assetCalls = [];
  const env = {
    BASE_PATH: "/onionflash", ARCHIVE_ORIGIN: origin, ARCHIVE_MANIFEST_SHA: rootHash,
    ARCHIVE_RETRY_ATTEMPTS: "1", ARCHIVE_BACKOFF_MS: "0", ARCHIVE_BACKOFF_CAP_MS: "0",
    ASSETS: { fetch(request) { assetCalls.push(request); return new Response("static", { status: 404 }); } },
    ...override,
  };
  const calls = [];
  const respond = (input, options = {}) => {
    const url = String(input);
    calls.push({ url, options });
    const obj = objects.get(url);
    if (!obj) return new Response("storage access denied", { status: 403 });
    const headers = { "content-length": String(obj.body.length), "content-type": "application/octet-stream", "set-cookie": "upstream=must-not-leak" };
    if (obj.sha256) headers["x-amz-meta-sha256"] = obj.sha256;
    return new Response(options.method === "HEAD" ? null : obj.body, { headers });
  };
  return { env, objects, rootHash, entries, calls, assetCalls, respond };
}

async function withOrigin(state, body, override) {
  const mocked = mock.method(globalThis, "fetch", override || state.respond);
  try { return await body(); } finally { mocked.mock.restore(); }
}

test("permanent article and feed paths resolve immutable manifest objects", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    for (const [path, key] of [["/news/a/", "news/a/index.html"], ["/news/a/story.json", "news/a/story.json"], ["/page/2/", "page/2/index.html"], ["/feed.xml", "feed.xml"], ["/feed.json", "feed.json"], ["/tags/security.xml", "tags/security.xml"]]) {
      const response = await worker.fetch(new Request("https://example.com/onionflash" + path + "?tracking=ignored"), state.env);
      assert.equal(response.status, 200);
      assert.equal(await response.text(), `public content for ${key}`);
      assert.equal(response.headers.get("content-type"), types[key]);
      assert.equal(response.headers.get("etag"), `"${state.entries[key].sha256}"`);
      assert.equal(response.headers.get("set-cookie"), null);
      assert.equal(response.headers.get("x-content-type-options"), "nosniff");
    }
    assert.equal(state.assetCalls.length, 0);
    assert.ok(state.calls.every(call => call.url.startsWith(origin) && !call.url.includes("?")));
  });
});

test("archive requests do not forward credentials or arbitrary request headers", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    const request = new Request("https://example.com/onionflash/news/a/", { headers: { Authorization: "Bearer secret", Cookie: "session=secret", Range: "bytes=0-1", "X-Original": "private" } });
    assert.equal((await worker.fetch(request, state.env)).status, 200);
    for (const call of state.calls) {
      assert.deepEqual(call.options.headers, { "Accept-Encoding": "identity" });
      assert.equal(call.options.redirect, "manual");
      assert.equal(call.options.cf.cacheEverything, true);
    }
  });
});

test("HEAD and conditional requests preserve bounded metadata semantics", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    const url = "https://example.com/onionflash/feed.xml";
    const head = await worker.fetch(new Request(url, { method: "HEAD" }), state.env);
    assert.equal(head.status, 200);
    assert.equal(await head.text(), "");
    assert.equal(head.headers.get("content-length"), String(state.entries["feed.xml"].size));
    assert.equal(state.calls.at(-1).options.method, "HEAD");
    state.calls.length = 0;
    const response = await worker.fetch(new Request(url, { headers: { "If-None-Match": head.headers.get("etag") } }), state.env);
    assert.equal(response.status, 304);
    assert.equal(await response.text(), "");
    assert.equal(response.headers.get("content-length"), null);
    assert.equal(state.calls.length, 2);
  });
});

test("HTML, XML and JSON get distinct safe response policies", async () => {
  // Roots written by earlier releases record the RSS type; the response type follows the path.
  const state = fixture({}, {}, { "feed.xml": "application/rss+xml; charset=utf-8" });
  await withOrigin(state, async () => {
    const html = await worker.fetch(new Request("https://example.com/onionflash/news/a/"), state.env);
    const xml = await worker.fetch(new Request("https://example.com/onionflash/feed.xml"), state.env);
    const json = await worker.fetch(new Request("https://example.com/onionflash/feed.json"), state.env);
    assert.match(html.headers.get("content-security-policy"), /script-src 'none'/);
    assert.match(html.headers.get("content-security-policy"), /connect-src 'none'/);
    assert.match(html.headers.get("content-security-policy"), /style-src 'self'/);
    assert.equal(xml.status, 200);
    assert.equal(xml.headers.get("content-type"), "application/xml; charset=utf-8");
    assert.match(xml.headers.get("content-security-policy"), /script-src 'none'/);
    assert.match(xml.headers.get("content-security-policy"), /style-src 'unsafe-inline'; img-src data:/);
    assert.match(json.headers.get("content-security-policy"), /script-src 'none'/);
    assert.doesNotMatch(json.headers.get("content-security-policy"), /unsafe-inline/);
  });
});

test("static topic pages resolve exact immutable entries with query-independent GET and HEAD", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    for (const key of ["topics/security/index.html", "topics/security/page/2/index.html"]) {
      const url = "https://example.com/onionflash/" + key.replace("index.html", "") + "?same=1&same=2";
      const get = await worker.fetch(new Request(url), state.env);
      assert.equal(get.status, 200);
      assert.equal(await get.text(), `public content for ${key}`);
      assert.equal(get.headers.get("content-type"), types[key]);
      assert.match(get.headers.get("content-security-policy"), /script-src 'none'/);
      assert.match(get.headers.get("content-security-policy"), /connect-src 'none'/);
      const head = await worker.fetch(new Request(url, { method: "HEAD" }), state.env);
      assert.equal(head.status, 200);
      assert.equal(await head.text(), "");
      assert.equal(head.headers.get("content-length"), String(state.entries[key].size));
      assert.equal(head.headers.get("etag"), get.headers.get("etag"));
      assert.equal(state.calls.at(-1).options.method, "HEAD");
      const cached = await worker.fetch(new Request(url, { headers: { "If-None-Match": head.headers.get("etag") } }), state.env);
      assert.equal(cached.status, 304);
      assert.equal(await cached.text(), "");
    }
    assert.equal(state.assetCalls.length, 0);
    assert.ok(state.calls.every(call => call.url.startsWith(origin) && !call.url.includes("?")));
  });
});

test("topic canonical redirects preserve query and page one collapses to the topic root", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    for (const method of ["GET", "HEAD"]) {
      for (const [path, destination] of [
        ["topics/security", "topics/security/"], ["topics/security/index.html", "topics/security/"],
        ["topics/security/page/1", "topics/security/"], ["topics/security/page/1/", "topics/security/"],
        ["topics/security/page/1/index.html", "topics/security/"],
        ["topics/security/page/2", "topics/security/page/2/"],
        ["topics/security/page/2/index.html", "topics/security/page/2/"],
      ]) {
        const response = await worker.fetch(new Request("https://example.com/onionflash/" + path + "?q=a%2Fb&same=1&same=2", { method }), state.env);
        assert.equal(response.status, 308);
        assert.equal(response.headers.get("location"), "https://example.com/onionflash/" + destination + "?q=a%2Fb&same=1&same=2");
        assert.equal(await response.text(), "");
      }
    }
    assert.equal(state.calls.length, 0);
    assert.equal(state.assetCalls.length, 0);
  });
});

test("article and pagination redirects preserve query and canonical locations", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    for (const [path, destination] of [["news/a", "news/a/"], ["news/a/index.html", "news/a/"], ["page/2", "page/2/"], ["page/2/index.html", "page/2/"], ["page/1/", ""]]) {
      const response = await worker.fetch(new Request("https://example.com/onionflash/" + path + "?same=1&same=2"), state.env);
      assert.equal(response.status, 308);
      assert.equal(response.headers.get("location"), "https://example.com/onionflash/" + destination + "?same=1&same=2");
    }
    assert.equal(state.calls.length, 0);
  });
});

test("private paths, encoded escapes, arbitrary topics and unbounded page numbers are never fetched", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    for (const path of ["private/evidence.json.gz", "news/a/%2e%2e/%2e%2e/private/key", "news/%2Fprivate/", "news/a/anything", "tags/private.xml", "page/0002/", "page/1000000000/", "objects/" + "a".repeat(64),
      "topics/private/", "topics/bridges/", "topics/bridges/page/2/", "topics/Security/", "topics/security-extra/", "topics/security/story.json", "topics/security/%2e%2e/%2e%2e/private/key", "topics/%2Fprivate/", "topics/security/page/0/", "topics/security/page/0002/", "topics/security/page/1000000000/", "topics/security/page/-1/", "topics/security/page/2/extra"]) {
      const response = await worker.fetch(new Request("https://example.com/onionflash/" + path), state.env);
      assert.equal(response.status, 404);
    }
    assert.equal(state.calls.length, 0);
  });
});

test("configured topic names use exact safe membership without regular-expression interpolation", async () => {
  const state = fixture({ ARCHIVE_TOPICS: "security,bridges" });
  await withOrigin(state, async () => {
    const response = await worker.fetch(new Request("https://example.com/onionflash/tags/bridges.xml"), state.env);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), "public content for tags/bridges.xml");
    for (const path of ["topics/bridges/", "topics/bridges/page/2/"]) {
      const page = await worker.fetch(new Request("https://example.com/onionflash/" + path), state.env);
      assert.equal(page.status, 200);
      assert.equal(await page.text(), `public content for ${path}index.html`);
    }
    state.calls.length = 0;
    for (const configured of ["security|private", "security,../private", "security,.*", "x".repeat(5000)]) {
      for (const path of ["tags/private.xml", "topics/private/", "topics/security/", "topics/security/page/2/"]) {
        const rejected = await worker.fetch(new Request("https://example.com/onionflash/" + path), { ...state.env, ARCHIVE_TOPICS: configured });
        assert.equal(rejected.status, 404);
      }
    }
    assert.equal(state.calls.length, 0);
  });
});

test("POST cannot fetch an archive object or consume its body", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    for (const path of ["news/a/", "topics/security/", "topics/security/page/2", "topics/security/page/2/"]) {
      const request = new Request("https://example.com/onionflash/" + path, { method: "POST", body: "keep private" });
      const response = await worker.fetch(request, state.env);
      assert.equal(response.status, 405);
      assert.equal(request.bodyUsed, false);
    }
    assert.equal(state.calls.length, 0);
  });
});

test("missing manifest shards and entries are plain 404s without object requests", async () => {
  const state = fixture();
  // news/removed/index.html shares news/a/index.html's shard but has no entry of its own.
  assert.equal(hash("news/removed/index.html").slice(0, 2), hash("news/a/index.html").slice(0, 2));
  await withOrigin(state, async () => {
    for (const path of ["news/missing/", "news/removed/"]) {
      const response = await worker.fetch(new Request("https://example.com/onionflash/" + path), state.env);
      assert.equal(response.status, 404);
      assert.equal(await response.text(), "Not found");
    }
    assert.ok(state.calls.every(call => call.url.includes("/manifests/")));
  });
});

test("wrong manifest bytes and oversized manifests fail closed without a retry", async () => {
  for (const kind of ["hash", "oversized"]) {
    const state = fixture({ ARCHIVE_RETRY_ATTEMPTS: "2" });
    const root = `${origin}manifests/${state.rootHash}.json`;
    const text = new TextDecoder().decode(state.objects.get(root).body);
    // Only the guard under test refuses each: the pinned root plus a space, or a pinned root past the limit.
    if (kind === "hash") state.objects.set(root, { body: new TextEncoder().encode(text + " ") });
    else {
      state.env.ARCHIVE_MAX_MANIFEST_BYTES = "1024";
      const padded = new TextEncoder().encode(text.padEnd(2048));
      state.env.ARCHIVE_MANIFEST_SHA = hash(padded);
      state.objects.set(`${origin}manifests/${hash(padded)}.json`, { body: padded });
    }
    await withOrigin(state, async () => {
      const response = await worker.fetch(new Request("https://example.com/onionflash/feed.xml"), state.env);
      assert.equal(response.status, 503);
      assert.equal(await response.text(), "Temporarily unavailable");
      assert.equal(state.calls.length, 1);
    });
  }
});

test("only transient origin failures are retried, within the configured limit", async () => {
  for (const [target, respond, attempts] of [
    ["/manifests/", () => new Response("temporary", { status: 503 }), 2],
    ["/manifests/", () => new Response(new ReadableStream({ pull(controller) { controller.error(new Error("connection reset")); } })), 2],
    ["/manifests/", () => new Response("storage access denied", { status: 403 }), 1],
    ["/manifests/", () => new Response(null, { status: 302, headers: { Location: "https://evil.example/private" } }), 1],
    ["/objects/", () => new Response("wrong object"), 1],
  ]) {
    const state = fixture({ ARCHIVE_RETRY_ATTEMPTS: "2" });
    let calls = 0;
    await withOrigin(state, async () => {
      assert.equal((await worker.fetch(new Request("https://example.com/onionflash/feed.xml"), state.env)).status, 503);
      assert.equal(calls, attempts);
    }, async (input, options) => String(input).includes(target) ? (calls++, respond()) : state.respond(input, options));
  }
});

test("an upstream error body is never exposed", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    const response = await worker.fetch(new Request("https://example.com/onionflash/feed.xml"), state.env);
    assert.equal(response.status, 404);
    assert.equal(await response.text(), "Not found");
  }, (input, options) => String(input).includes("/objects/") ? new Response("private storage diagnostic", { status: 403 }) : state.respond(input, options));
});

test("a rolled-back root keeps retrieving the previous immutable article", async () => {
  const old = fixture();
  const newer = fixture({}, { "news/a/index.html": "corrected article" });
  for (const [key, value] of old.objects) if (!newer.objects.has(key)) newer.objects.set(key, value);
  // The selected root, rather than a mutable object path, controls the lookup.
  const oldBody = old.objects.get(`${origin}objects/${old.entries["news/a/index.html"].sha256}`).body;
  await withOrigin(newer, async () => {
    const response = await worker.fetch(new Request("https://example.com/onionflash/news/a/"), old.env);
    assert.equal(response.status, 200);
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()), oldBody);
    assert.equal(newer.calls[0].url, `${origin}manifests/${old.rootHash}.json`);
    const current = await worker.fetch(new Request("https://example.com/onionflash/news/a/"), newer.env);
    assert.equal(await current.text(), "corrected article");
    assert.notEqual(old.rootHash, newer.rootHash);
  });
});

test("an object whose bytes or metadata differ from its manifest entry is never served", async () => {
  const state = fixture({}, { "feed.xml": "test" });
  const sha = state.entries["feed.xml"].sha256;
  // Replaced, short and long bodies carry the publisher's metadata; only their bytes differ.
  for (const [body, length, metadata, methods] of [
    ["TEST", "4", sha, ["GET"]], ["tes", "4", sha, ["GET"]], ["test!", "4", sha, ["GET"]],
    ["test", "5", sha, ["GET", "HEAD"]], ["test", "4", "0".repeat(64), ["GET", "HEAD"]], ["test", "4", null, ["GET", "HEAD"]],
  ]) {
    for (const method of methods) {
      await withOrigin(state, async () => {
        const response = await worker.fetch(new Request("https://example.com/onionflash/feed.xml", { method }), state.env);
        assert.equal(response.status, 503, `${method} ${body} ${length} ${metadata}`);
      }, (input, options) => {
        if (!String(input).includes("/objects/")) return state.respond(input, options);
        const headers = { "content-length": length, ...(metadata ? { "x-amz-meta-sha256": metadata } : {}) };
        return new Response(method === "HEAD" ? null : body, { headers });
      });
    }
  }
});
