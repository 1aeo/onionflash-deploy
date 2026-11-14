import assert from "node:assert/strict";
import { test } from "node:test";
import worker from "../redirect-worker.mjs";
import { withFetch } from "./worker_helpers.mjs";

const env = { CANONICAL_URL: "https://example.com/onionflash/" };

test("alias root, articles, feeds, prefixes and lookalike paths redirect cleanly", async () => {
  for (const [path, expected] of [
    ["/", "/onionflash/"], ["/news/abc/", "/onionflash/news/abc/"], ["/feed.xml", "/onionflash/feed.xml"],
    ["/feed.json", "/onionflash/feed.json"], ["/tags/security.xml", "/onionflash/tags/security.xml"],
    ["/onionflash", "/onionflash/"], ["/onionflash/", "/onionflash/"], ["/onionflash/news/abc/", "/onionflash/news/abc/"],
    ["/onionflash/feed.xml", "/onionflash/feed.xml"], ["/onionflash-other", "/onionflash/onionflash-other"],
    ["/onionflashish/feed.xml", "/onionflash/onionflashish/feed.xml"], ["/onionflash%2Ffeed.xml", "/onionflash/onionflash%2Ffeed.xml"],
  ]) {
    const response = await worker.fetch(new Request(`https://onionflash.example.com${path}`), env);
    assert.equal(response.status, 308);
    assert.equal(response.headers.get("location"), `https://example.com${expected}`);
  }
});

test("HTTP, preview origins and query strings redirect to canonical HTTPS with exact queries", async () => {
  for (const origin of ["http://onionflash.example.com", "https://candidate.alias.workers.dev"]) {
    const response = await worker.fetch(new Request(origin + "/news/abc/"), env);
    assert.equal(response.status, 308);
    assert.equal(response.headers.get("location"), "https://example.com/onionflash/news/abc/");
  }
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

test("canonical root/nested mounts work and redirecting never fetches or reads body", async () => {
  for (const base of ["https://example.com/", "https://example.com/tools/news/"]) {
    for (const path of ["/", "/news/abc/", "/feed.xml?reader=1"]) {
      const response = await worker.fetch(new Request(`https://onionflash.example.com${path}`), { CANONICAL_URL: base });
      assert.equal(response.headers.get("location"), base + path.slice(1));
    }
  }
  await withFetch(async () => { throw new Error("redirect must not fetch"); }, async origin => {
    const request = new Request("https://onionflash.example.com/news/abc/", { method: "POST", body: "original payload" });
    const response = await worker.fetch(request, env);
    assert.equal(response.status, 308);
    assert.equal(origin.mock.callCount(), 0);
    assert.equal(request.bodyUsed, false);
    assert.equal(await request.text(), "original payload");
  });
});
