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
  "feed.xml": "application/rss+xml; charset=utf-8",
  "tags/security.xml": "application/rss+xml; charset=utf-8",
  "tags/bridges.xml": "application/rss+xml; charset=utf-8",
  "feed.json": "application/feed+json; charset=utf-8",
};

function fixture(override = {}, contents = {}) {
  const objects = new Map();
  const shards = {};
  const entries = {};
  for (const [key, content_type] of Object.entries(types)) {
    const body = new TextEncoder().encode(contents[key] ?? `public content for ${key}`);
    const sha256 = hash(body);
    entries[key] = { sha256, size: body.length, content_type };
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
      assert.equal(call.options.redirect, "error");
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
  const state = fixture();
  await withOrigin(state, async () => {
    const html = await worker.fetch(new Request("https://example.com/onionflash/news/a/"), state.env);
    const xml = await worker.fetch(new Request("https://example.com/onionflash/feed.xml"), state.env);
    const json = await worker.fetch(new Request("https://example.com/onionflash/feed.json"), state.env);
    assert.match(html.headers.get("content-security-policy"), /script-src 'self'/);
    assert.match(xml.headers.get("content-security-policy"), /script-src 'none'/);
    assert.match(xml.headers.get("content-security-policy"), /style-src 'unsafe-inline'/);
    assert.match(json.headers.get("content-security-policy"), /script-src 'none'/);
    assert.doesNotMatch(json.headers.get("content-security-policy"), /unsafe-inline/);
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
    for (const path of ["private/evidence.json.gz", "news/a/%2e%2e/%2e%2e/private/key", "news/%2Fprivate/", "news/a/anything", "tags/private.xml", "page/0002/", "page/1000000000/", "objects/" + "a".repeat(64)]) {
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
    state.calls.length = 0;
    for (const configured of ["security|private", "security,../private", "security,.*", "x".repeat(5000)]) {
      const rejected = await worker.fetch(new Request("https://example.com/onionflash/tags/private.xml"), { ...state.env, ARCHIVE_TOPICS: configured });
      assert.equal(rejected.status, 404);
    }
    assert.equal(state.calls.length, 0);
  });
});

test("POST cannot fetch an archive object or consume its body", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    const request = new Request("https://example.com/onionflash/news/a/", { method: "POST", body: "keep private" });
    const response = await worker.fetch(request, state.env);
    assert.equal(response.status, 405);
    assert.equal(request.bodyUsed, false);
    assert.equal(state.calls.length, 0);
  });
});

test("missing manifest entries are plain 404s without arbitrary object requests", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    const response = await worker.fetch(new Request("https://example.com/onionflash/news/missing/"), state.env);
    assert.equal(response.status, 404);
    assert.equal(await response.text(), "Not found");
    assert.ok(state.calls.every(call => call.url.includes("/manifests/")));
  });
});

test("untrusted redirects, wrong hashes and oversized manifests fail closed", async () => {
  for (const kind of ["redirect", "hash", "oversized"]) {
    const state = fixture({ ARCHIVE_MAX_MANIFEST_BYTES: "1024" });
    await withOrigin(state, async () => {
      const response = await worker.fetch(new Request("https://example.com/onionflash/feed.xml"), state.env);
      assert.equal(response.status, 503);
      assert.equal(await response.text(), "Temporarily unavailable");
    }, async () => kind === "redirect" ? new Response(null, { status: 302, headers: { Location: "https://evil.example/private" } })
      : kind === "oversized" ? new Response("x", { headers: { "content-length": "100000" } }) : new Response('{"wrong":"data"}'));
  }
});

test("origin failure retries only within the configured limit", async () => {
  const state = fixture({ ARCHIVE_RETRY_ATTEMPTS: "2" });
  let attempts = 0;
  await withOrigin(state, async () => {
    assert.equal((await worker.fetch(new Request("https://example.com/onionflash/feed.xml"), state.env)).status, 503);
    assert.equal(attempts, 2);
  }, async () => { attempts++; return new Response("temporary", { status: 503 }); });
});

test("object body streams and an upstream error body is never exposed", async () => {
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

test("an object that stalls after headers is cancelled within the idle-read deadline", async () => {
  const state = fixture({ ARCHIVE_TIMEOUT_MS: "100" }, { "feed.xml": "test" });
  let cancelled = false;
  let signal;
  const stream = new ReadableStream({
    start(controller) { controller.enqueue(new Uint8Array([116])); },
    pull() { return new Promise(() => {}); },
    cancel() { cancelled = true; },
  });
  await withOrigin(state, async () => {
    const response = await worker.fetch(new Request("https://example.com/onionflash/feed.xml"), state.env);
    assert.equal(response.status, 200);
    const started = Date.now();
    await assert.rejects(response.arrayBuffer(), /read timed out/);
    assert.ok(Date.now() - started < 1000, "stalled body must not hang after headers");
    assert.equal(cancelled, true);
    assert.equal(signal.aborted, true);
    assert.equal(stream.locked, false);
  }, (input, options) => {
    if (!String(input).includes("/objects/")) return state.respond(input, options);
    signal = options.signal;
    return new Response(stream, { headers: { "content-length": "4", "x-amz-meta-sha256": state.entries["feed.xml"].sha256 } });
  });
});

test("slow consumer backpressure does not start an origin idle-read timeout", async () => {
  const state = fixture({ ARCHIVE_TIMEOUT_MS: "100" }, { "feed.xml": "test" });
  let position = 0;
  let cancelled = false;
  let signal;
  const bytes = new TextEncoder().encode("test");
  const stream = new ReadableStream({
    pull(controller) {
      if (position === bytes.length) controller.close();
      else controller.enqueue(bytes.subarray(position, ++position));
    },
    cancel() { cancelled = true; },
  }, { highWaterMark: 0 });
  await withOrigin(state, async () => {
    const response = await worker.fetch(new Request("https://example.com/onionflash/feed.xml"), state.env);
    const reader = response.body.getReader();
    const first = await reader.read();
    assert.deepEqual([...first.value], [116]);
    await new Promise(resolve => setTimeout(resolve, 150));
    assert.equal(position, 1, "do not prefetch while the consumer has no demand");
    assert.equal(signal.aborted, false);
    const rest = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      rest.push(...value);
    }
    assert.equal(new TextDecoder().decode(new Uint8Array([116, ...rest])), "test");
    assert.equal(cancelled, false);
    assert.equal(stream.locked, false);
  }, (input, options) => {
    if (!String(input).includes("/objects/")) return state.respond(input, options);
    signal = options.signal;
    return new Response(stream, { headers: { "content-length": "4", "x-amz-meta-sha256": state.entries["feed.xml"].sha256 } });
  });
});

test("cancelling a client read and unexpected HEAD bodies release the origin", async () => {
  for (const method of ["GET", "HEAD"]) {
    const state = fixture({ ARCHIVE_TIMEOUT_MS: "100" }, { "feed.xml": "test" });
    let cancelled = false;
    let signal;
    const stream = new ReadableStream({ cancel() { cancelled = true; } });
    await withOrigin(state, async () => {
      const response = await worker.fetch(new Request("https://example.com/onionflash/feed.xml", { method }), state.env);
      assert.equal(response.status, 200);
      if (method === "GET") await response.body.cancel("reader left");
      else assert.equal(await response.text(), "");
      assert.equal(cancelled, true);
      assert.equal(signal.aborted, true);
      assert.equal(stream.locked, false);
    }, (input, options) => {
      if (!String(input).includes("/objects/")) return state.respond(input, options);
      signal = options.signal;
      return new Response(stream, { headers: { "content-length": "4", "x-amz-meta-sha256": state.entries["feed.xml"].sha256 } });
    });
  }
});
