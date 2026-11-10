import assert from "node:assert/strict";
import { mock, test } from "node:test";
import worker from "../redirect-worker.mjs";

const env = { CANONICAL_URL: "https://example.com/onionflash/" };

test("alias root, articles and feeds redirect to the canonical mount", async () => {
  for (const [path, expected] of [
    ["/", "/onionflash/"],
    ["/news/abc/", "/onionflash/news/abc/"],
    ["/feed.xml", "/onionflash/feed.xml"],
    ["/feed.json", "/onionflash/feed.json"],
    ["/tags/security.xml", "/onionflash/tags/security.xml"],
  ]) {
    const response = await worker.fetch(new Request(`https://onionflash.example.com${path}`), env);
    assert.equal(response.status, 308);
    assert.equal(response.headers.get("location"), `https://example.com${expected}`);
  }
});

test("existing mount prefixes are not duplicated and lookalike paths stay inside the app", async () => {
  for (const [path, expected] of [
    ["/onionflash", "/onionflash/"],
    ["/onionflash/", "/onionflash/"],
    ["/onionflash/news/abc/", "/onionflash/news/abc/"],
    ["/onionflash/feed.xml", "/onionflash/feed.xml"],
    ["/onionflash-other", "/onionflash/onionflash-other"],
    ["/onionflashish/feed.xml", "/onionflash/onionflashish/feed.xml"],
    ["/onionflash%2Ffeed.xml", "/onionflash/onionflash%2Ffeed.xml"],
  ]) {
    const response = await worker.fetch(new Request(`https://onionflash.example.com${path}`), env);
    assert.equal(response.headers.get("location"), `https://example.com${expected}`);
  }
});

test("HTTP and Worker preview requests redirect to canonical HTTPS", async () => {
  for (const origin of ["http://onionflash.example.com", "https://candidate.alias.workers.dev"]) {
    const response = await worker.fetch(new Request(origin + "/news/abc/"), env);
    assert.equal(response.status, 308);
    assert.equal(response.headers.get("location"), "https://example.com/onionflash/news/abc/");
  }
});

test("query strings remain exact including duplicate keys and encoded URLs", async () => {
  for (const query of ["", "?q=a%2Fb&same=1&same=2", "?next=https%3A%2F%2Fevil.example%2F&x=%0D%0A"]) {
    const response = await worker.fetch(new Request(`https://onionflash.example.com/feed.xml${query}`), env);
    assert.equal(response.headers.get("location"), `https://example.com/onionflash/feed.xml${query}`);
  }
});

test("host-like and encoded paths cannot become an open redirect", async () => {
  for (const path of ["//evil.example/path", "/%2F%2Fevil.example/path", "/https://evil.example/", "/onionflash/../outside", "/%0D%0ALocation:evil"]) {
    const response = await worker.fetch(new Request(`https://onionflash.example.com${path}`), env);
    const target = new URL(response.headers.get("location"));
    assert.equal(response.status, 308);
    assert.equal(target.origin, "https://example.com");
    assert.ok(target.pathname.startsWith("/onionflash/"));
    assert.equal(target.username, "");
    assert.equal(target.password, "");
  }
});

test("a canonical root or nested mount handles ordinary paths", async () => {
  for (const base of ["https://example.com/", "https://example.com/tools/news/"]) {
    for (const path of ["/", "/news/abc/", "/feed.xml?reader=1"]) {
      const response = await worker.fetch(new Request(`https://onionflash.example.com${path}`), { CANONICAL_URL: base });
      assert.equal(response.headers.get("location"), base + path.slice(1));
    }
  }
});

test("redirecting never fetches an origin or consumes the request body", async () => {
  const origin = mock.method(globalThis, "fetch", async () => {
    throw new Error("redirect must not fetch");
  });
  try {
    const request = new Request("https://onionflash.example.com/news/abc/", { method: "POST", body: "original payload" });
    const response = await worker.fetch(request, env);
    assert.equal(response.status, 308);
    assert.equal(origin.mock.callCount(), 0);
    assert.equal(request.bodyUsed, false);
    assert.equal(await request.text(), "original payload");
  } finally {
    origin.mock.restore();
  }
});
