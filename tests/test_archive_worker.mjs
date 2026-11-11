import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mock, test } from "node:test";
import worker from "../worker.mjs";

const hash = value => createHash("sha256").update(value).digest("hex");
const origin = "https://public.example/onionflash/public/";
const types = {
  "news/a/index.html": "text/html; charset=utf-8",
  "news/a/story.json": "application/json; charset=utf-8",
};

function fixture(contents = {}) {
  const objects = new Map();
  const shards = {};
  const entries = {};
  for (const key of Object.keys(types)) {
    const body = new TextEncoder().encode(contents[key] ?? `public content for ${key}`);
    const sha256 = hash(body);
    entries[key] = { sha256, size: body.length };
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
    ASSETS: { fetch(request) { assetCalls.push(request); return new Response("static", { status: 404 }); } },
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

test("article permalinks resolve immutable manifest objects with safe headers", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    for (const [path, key] of [["/news/a/", "news/a/index.html"], ["/news/a/story.json", "news/a/story.json"]]) {
      const response = await worker.fetch(new Request("https://example.com/onionflash" + path + "?tracking=ignored"), state.env);
      assert.equal(response.status, 200);
      assert.equal(await response.text(), `public content for ${key}`);
      assert.equal(response.headers.get("content-type"), types[key]);
      assert.equal(response.headers.get("etag"), `"${state.entries[key].sha256}"`);
      assert.equal(response.headers.get("set-cookie"), null);
      assert.equal(response.headers.get("x-content-type-options"), "nosniff");
      assert.match(response.headers.get("content-security-policy"), /script-src 'none'/);
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
    const url = "https://example.com/onionflash/news/a/story.json";
    const head = await worker.fetch(new Request(url, { method: "HEAD" }), state.env);
    assert.equal(head.status, 200);
    assert.equal(await head.text(), "");
    assert.equal(head.headers.get("content-length"), String(state.entries["news/a/story.json"].size));
    assert.equal(state.calls.at(-1).options.method, "HEAD");
    state.calls.length = 0;
    const response = await worker.fetch(new Request(url, { headers: { "If-None-Match": head.headers.get("etag") } }), state.env);
    assert.equal(response.status, 304);
    assert.equal(await response.text(), "");
    assert.equal(response.headers.get("content-length"), null);
    assert.equal(state.calls.length, 2);
  });
});

test("HTML and JSON get distinct safe response policies", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    const html = await worker.fetch(new Request("https://example.com/onionflash/news/a/"), state.env);
    const json = await worker.fetch(new Request("https://example.com/onionflash/news/a/story.json"), state.env);
    assert.match(html.headers.get("content-security-policy"), /connect-src 'none'/);
    assert.match(html.headers.get("content-security-policy"), /style-src 'self'/);
    assert.doesNotMatch(json.headers.get("content-security-policy"), /style-src|unsafe-inline/);
  });
});

test("canonical article redirects preserve the query for GET and HEAD", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    for (const method of ["GET", "HEAD"]) {
      for (const path of ["news/a", "news/a/index.html"]) {
        const response = await worker.fetch(new Request("https://example.com/onionflash/" + path + "?q=a%2Fb&same=1&same=2", { method }), state.env);
        assert.equal(response.status, 308);
        assert.equal(response.headers.get("location"), "https://example.com/onionflash/news/a/?q=a%2Fb&same=1&same=2");
        assert.equal(await response.text(), "");
      }
    }
    assert.equal(state.calls.length, 0);
    assert.equal(state.assetCalls.length, 0);
  });
});

test("listings, feeds, private paths and encoded escapes never reach the origin", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    for (const path of ["", "feed.xml", "page/2/", "topics/security/", "tags/security.xml", "private/evidence.json.gz",
      "news/a/%2e%2e/%2e%2e/private/key", "news/%2Fprivate/", "news/a/anything", "objects/" + "a".repeat(64)]) {
      const response = await worker.fetch(new Request("https://example.com/onionflash/" + path), state.env);
      assert.equal(response.status, 404);
    }
    assert.equal(state.assetCalls.length, 10);
    assert.equal(state.calls.length, 0);
  });
});

test("POST cannot fetch an archive object or consume its body", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    for (const path of ["news/a/", "news/a/story.json"]) {
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
    const state = fixture();
    const root = `${origin}manifests/${state.rootHash}.json`;
    const text = new TextDecoder().decode(state.objects.get(root).body);
    // Only the guard under test refuses each: the pinned root plus a space, or a pinned root past the limit.
    if (kind === "hash") state.objects.set(root, { body: new TextEncoder().encode(text + " ") });
    else {
      const padded = new TextEncoder().encode(text.padEnd(8 * 1024 * 1024 + 1));
      state.env.ARCHIVE_MANIFEST_SHA = hash(padded);
      state.objects.set(`${origin}manifests/${hash(padded)}.json`, { body: padded });
    }
    await withOrigin(state, async () => {
      const response = await worker.fetch(new Request("https://example.com/onionflash/news/a/"), state.env);
      assert.equal(response.status, 503);
      assert.equal(await response.text(), "Temporarily unavailable");
      assert.equal(state.calls.length, 1);
    });
  }
});

test("only transient origin failures are retried, within the retry limit", async () => {
  for (const [target, respond, attempts] of [
    ["/manifests/", () => new Response("temporary", { status: 503 }), 2],
    ["/manifests/", () => new Response(new ReadableStream({ pull(controller) { controller.error(new Error("connection reset")); } })), 2],
    ["/manifests/", () => new Response("storage access denied", { status: 403 }), 1],
    ["/manifests/", () => new Response(null, { status: 302, headers: { Location: "https://evil.example/private" } }), 1],
    ["/objects/", () => new Response("wrong object"), 1],
  ]) {
    const state = fixture();
    let calls = 0;
    await withOrigin(state, async () => {
      assert.equal((await worker.fetch(new Request("https://example.com/onionflash/news/a/"), state.env)).status, 503);
      assert.equal(calls, attempts);
    }, async (input, options) => String(input).includes(target) ? (calls++, respond()) : state.respond(input, options));
  }
});

test("an upstream error body is never exposed", async () => {
  const state = fixture();
  await withOrigin(state, async () => {
    const response = await worker.fetch(new Request("https://example.com/onionflash/news/a/"), state.env);
    assert.equal(response.status, 404);
    assert.equal(await response.text(), "Not found");
  }, (input, options) => String(input).includes("/objects/") ? new Response("private storage diagnostic", { status: 403 }) : state.respond(input, options));
});

test("a rolled-back root keeps retrieving the previous immutable article", async () => {
  const old = fixture();
  const newer = fixture({ "news/a/index.html": "corrected article" });
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
  const state = fixture({ "news/a/story.json": "test" });
  const sha = state.entries["news/a/story.json"].sha256;
  // Replaced, short and long bodies carry the publisher's metadata; only their bytes differ.
  for (const [body, length, metadata, methods] of [
    ["TEST", "4", sha, ["GET"]], ["tes", "4", sha, ["GET"]], ["test!", "4", sha, ["GET"]],
    ["test", "5", sha, ["GET", "HEAD"]], ["test", "4", "0".repeat(64), ["GET", "HEAD"]], ["test", "4", null, ["GET", "HEAD"]],
  ]) {
    for (const method of methods) {
      await withOrigin(state, async () => {
        const response = await worker.fetch(new Request("https://example.com/onionflash/news/a/story.json", { method }), state.env);
        assert.equal(response.status, 503, `${method} ${body} ${length} ${metadata}`);
      }, (input, options) => {
        if (!String(input).includes("/objects/")) return state.respond(input, options);
        const headers = { "content-length": length, ...(metadata ? { "x-amz-meta-sha256": metadata } : {}) };
        return new Response(method === "HEAD" ? null : body, { headers });
      });
    }
  }
});
