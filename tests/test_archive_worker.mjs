import assert from "node:assert/strict";
import { test } from "node:test";
import { gzipSync } from "node:zlib";
import worker from "../worker.mjs";
import { ORIGIN, TYPES, archiveFixture, sha256Hex, withOrigin } from "./worker_helpers.mjs";

test("article permalinks resolve immutable gzipped manifest objects in 1 manifest hop and cache entries", async () => {
  const state = archiveFixture();
  await withOrigin(state, async () => {
    for (const [path, key, expectedCalls] of [
      ["/news/a/", "news/a/index.html", 2],
      ["/news/a/story.json", "news/a/story.json", 3],
    ]) {
      const response = await worker.fetch(new Request(`https://example.com/onionflash${path}?tracking=ignored`), state.env);
      assert.equal(response.status, 200);
      assert.equal(await response.text(), `public content for ${key}`);
      assert.equal(response.headers.get("content-type"), TYPES[key]);
      assert.equal(response.headers.get("etag"), `"${state.entries[key].sha256}"`);
      assert.equal(response.headers.get("set-cookie"), null);
      assert.equal(response.headers.get("x-content-type-options"), "nosniff");
      assert.match(response.headers.get("content-security-policy"), /script-src 'none'/);
      assert.equal(state.calls.length, expectedCalls);
    }
    assert.match(
      (await worker.fetch(new Request("https://example.com/onionflash/news/a/"), state.env)).headers.get("content-security-policy"),
      /connect-src 'none'.*style-src 'self'|style-src 'self'.*connect-src 'none'/,
    );
    assert.doesNotMatch(
      (await worker.fetch(new Request("https://example.com/onionflash/news/a/story.json"), state.env)).headers.get("content-security-policy"),
      /style-src|unsafe-inline/,
    );
    assert.equal(state.assetCalls.length, 0);
    assert.ok(state.calls.every(call => call.url.startsWith(ORIGIN) && !call.url.includes("?")));
  });
});

test("archive requests sanitize headers and HEAD/304 preserve bounded metadata", async () => {
  const state = archiveFixture();
  await withOrigin(state, async () => {
    const request = new Request("https://example.com/onionflash/news/a/", {
      headers: { Authorization: "Bearer secret", Cookie: "session=secret", Range: "bytes=0-1", "X-Original": "private" },
    });
    assert.equal((await worker.fetch(request, state.env)).status, 200);
    for (const call of state.calls) {
      assert.deepEqual(call.options.headers, { "Accept-Encoding": "identity" });
      assert.equal(call.options.redirect, "manual");
      assert.equal(call.options.cf.cacheEverything, true);
    }
    const url = "https://example.com/onionflash/news/a/story.json";
    const head = await worker.fetch(new Request(url, { method: "HEAD" }), state.env);
    assert.equal(head.status, 200);
    assert.equal(await head.text(), "");
    assert.equal(head.headers.get("content-length"), String(state.entries["news/a/story.json"].size));
    assert.equal(state.calls.at(-1).options.method, "HEAD");
    state.calls.length = 0;
    const cached = await worker.fetch(new Request(url, { headers: { "If-None-Match": head.headers.get("etag") } }), state.env);
    assert.equal(cached.status, 304);
    assert.equal(await cached.text(), "");
    assert.equal(cached.headers.get("content-length"), null);
    assert.equal(state.calls.length, 0);
  });
});

test("canonical article redirects, static routing boundaries, POST 405 and missing entries", async () => {
  const state = archiveFixture();
  await withOrigin(state, async () => {
    for (const method of ["GET", "HEAD"]) {
      for (const path of ["news/a", "news/a/index.html"]) {
        const response = await worker.fetch(new Request(`https://example.com/onionflash/${path}?q=a%2Fb&same=1&same=2`, { method }), state.env);
        assert.equal(response.status, 308);
        assert.equal(response.headers.get("location"), "https://example.com/onionflash/news/a/?q=a%2Fb&same=1&same=2");
      }
    }
    for (const path of ["", "feed.xml", "page/2/", "topics/security/", "tags/security.xml", "private/evidence.json.gz",
      "news/a/%2e%2e/%2e%2e/private/key", "news/%2Fprivate/", "news/a/anything", "objects/" + "a".repeat(64)]) {
      assert.equal((await worker.fetch(new Request("https://example.com/onionflash/" + path), state.env)).status, 404);
    }
    assert.equal(state.assetCalls.length, 10);
    for (const path of ["news/a/", "news/a/story.json"]) {
      const post = new Request("https://example.com/onionflash/" + path, { method: "POST", body: "keep private" });
      assert.equal((await worker.fetch(post, state.env)).status, 405);
      assert.equal(post.bodyUsed, false);
    }
    assert.equal(state.calls.length, 0);
    for (const path of ["news/missing/", "news/removed/"]) {
      const response = await worker.fetch(new Request("https://example.com/onionflash/" + path), state.env);
      assert.equal(response.status, 404);
      assert.equal(await response.text(), "Not found");
    }
    assert.equal(state.calls.length, 1);
    assert.ok(state.calls[0].url.endsWith(".json.gz"));
  });
});

test("bad manifest hashes, oversized/corrupt gzip streams and invalid schemas fail closed without retry", async () => {
  const putRawManifest = (state, bytes, keepSha = false) => {
    const sha = keepSha ? state.manifestSha : sha256Hex(bytes);
    state.env.ARCHIVE_MANIFEST_SHA = sha;
    state.objects.set(`${ORIGIN}manifests/${sha}.json.gz`, { body: bytes, sha256: sha });
  };
  for (const mutate of [
    state => putRawManifest(state, new Uint8Array([1, 2, 3]), true),
    state => putRawManifest(state, new Uint8Array(8 * 1024 * 1024 + 1)),
    state => putRawManifest(state, new Uint8Array(gzipSync(Buffer.alloc(16 * 1024 * 1024 + 64)))),
    state => putRawManifest(state, new Uint8Array([0x1f, 0x8b, 0x08, 0x00, 0xff])),
    state => putRawManifest(state, new Uint8Array(gzipSync(Buffer.from('{"format":"wrong","entries":{}}')))),
  ]) {
    const state = archiveFixture();
    mutate(state);
    await withOrigin(state, async () => {
      const response = await worker.fetch(new Request("https://example.com/onionflash/news/a/"), state.env);
      assert.equal(response.status, 503);
      assert.equal(await response.text(), "Temporarily unavailable");
      assert.equal(state.calls.length, 1);
    });
  }
});

test("transient origin failures retry within limit and upstream error bodies stay hidden", async () => {
  for (const [target, respond, attempts] of [
    ["/manifests/", () => new Response("temporary", { status: 503 }), 2],
    ["/manifests/", () => new Response(new ReadableStream({ pull(c) { c.error(new Error("connection reset")); } })), 2],
    ["/manifests/", () => new Response("storage access denied", { status: 403 }), 1],
    ["/manifests/", () => new Response(null, { status: 302, headers: { Location: "https://evil.example/private" } }), 1],
    ["/objects/", () => new Response("wrong object"), 1],
  ]) {
    const state = archiveFixture();
    let calls = 0;
    await withOrigin(state, async () => {
      assert.equal((await worker.fetch(new Request("https://example.com/onionflash/news/a/"), state.env)).status, 503);
      assert.equal(calls, attempts);
    }, async (input, options) => String(input).includes(target) ? (calls++, respond()) : state.respond(input, options));
  }
  const hidden = archiveFixture();
  await withOrigin(hidden, async () => {
    const response = await worker.fetch(new Request("https://example.com/onionflash/news/a/"), hidden.env);
    assert.equal(response.status, 404);
    assert.equal(await response.text(), "Not found");
  }, (input, options) => String(input).includes("/objects/") ? new Response("private storage diagnostic", { status: 403 }) : hidden.respond(input, options));
});

test("rolled-back manifests keep historical articles and tampered objects are never served", async () => {
  const old = archiveFixture();
  const newer = archiveFixture({ "news/a/index.html": "corrected article" });
  for (const [key, value] of old.objects) if (!newer.objects.has(key)) newer.objects.set(key, value);
  const oldBody = old.objects.get(`${ORIGIN}objects/${old.entries["news/a/index.html"].sha256}`).body;
  await withOrigin(newer, async () => {
    const response = await worker.fetch(new Request("https://example.com/onionflash/news/a/"), old.env);
    assert.equal(response.status, 200);
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()), oldBody);
    assert.equal(newer.calls[0].url, `${ORIGIN}manifests/${old.manifestSha}.json.gz`);
    assert.equal(await (await worker.fetch(new Request("https://example.com/onionflash/news/a/"), newer.env)).text(), "corrected article");
    assert.notEqual(old.manifestSha, newer.manifestSha);
  });
  const state = archiveFixture({ "news/a/story.json": "test" });
  const sha = state.entries["news/a/story.json"].sha256;
  for (const [body, length, metadata, methods] of [
    ["TEST", "4", sha, ["GET"]], ["tes", "4", sha, ["GET"]], ["test!", "4", sha, ["GET"]],
    ["test", "5", sha, ["GET", "HEAD"]], ["test", "4", "0".repeat(64), ["GET", "HEAD"]], ["test", "4", null, ["GET", "HEAD"]],
  ]) {
    for (const method of methods) {
      await withOrigin(state, async () => {
        const response = await worker.fetch(new Request("https://example.com/onionflash/news/a/story.json", { method }), state.env);
        assert.equal(response.status, 503);
      }, (input, options) => {
        if (!String(input).includes("/objects/")) return state.respond(input, options);
        const headers = { "content-length": length, ...(metadata ? { "x-amz-meta-sha256": metadata } : {}) };
        return new Response(method === "HEAD" ? null : body, { headers });
      });
    }
  }
});
