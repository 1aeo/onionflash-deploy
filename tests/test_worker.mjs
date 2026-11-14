import assert from "node:assert/strict";
import { test } from "node:test";
import worker from "../worker.mjs";
import { makeAssets, withFetch } from "./worker_helpers.mjs";

const noOrigin = async () => { throw new Error("unexpected origin request"); };

test("mount redirect preserves the full query string", async () => {
  const state = makeAssets();
  await withFetch(noOrigin, async origin => {
    for (const query of ["", "?ref=rss", "?q=a%2Fb&same=1&same=2", "?next=https%3A%2F%2Felsewhere.example%2F"]) {
      const response = await worker.fetch(new Request(`https://example.com/onionflash${query}`), state.env);
      assert.equal(response.status, 308);
      assert.equal(response.headers.get("location"), `https://example.com/onionflash/${query}`);
    }
    assert.equal(state.calls.length, 0);
    assert.equal(origin.mock.callCount(), 0);
  });
});

test("application subtree uses only the static assets binding", async () => {
  const state = makeAssets();
  await withFetch(noOrigin, async origin => {
    for (const [path, init] of [["/onionflash/"], ["/onionflash/feed.xml?reader=1"], ["/onionflash/news/abc/"], ["/onionflash/tags/security.xml"],
      ["/onionflash/topics/security/"], ["/onionflash/topics/security/page/2/?reader=1"], ["/onionflash/topics/security/page/2/?reader=1", { method: "HEAD" }]]) {
      const request = new Request(`https://example.com${path}`, init);
      const response = await worker.fetch(request, state.env);
      assert.equal(response.status, 200);
      assert.equal(state.calls.at(-1), request);
    }
    assert.equal(state.calls.length, 7);
    assert.equal(state.calls.at(-1).method, "HEAD");
    assert.equal(origin.mock.callCount(), 0);
  });
});

test("in-scope asset 404 does not fall back to the site's origin", async () => {
  const state = makeAssets(404);
  await withFetch(async () => new Response("origin must not be used"), async origin => {
    const request = new Request("https://example.com/onionflash/not-found");
    const response = await worker.fetch(request, state.env);
    assert.equal(response.status, 404);
    assert.equal(await response.text(), "not found");
    assert.equal(state.calls[0], request);
    assert.equal(origin.mock.callCount(), 0);
  });
});

test("similar and unrelated paths preserve the original request", async () => {
  const state = makeAssets();
  const originResponse = new Response("origin content", { status: 202 });
  await withFetch(async () => originResponse, async origin => {
    for (const path of ["/onionflash-old", "/onionflashish/feed.xml", "/onionflash%2Ffeed.xml", "/ONIONFLASH/", "/other?source=onionflash", "/onionflash/../other"]) {
      const request = new Request(`https://example.com${path}`, {
        method: "POST", headers: { "X-Original-Header": "preserved" }, body: "preserve this body",
      });
      const response = await worker.fetch(request, state.env);
      assert.equal(response, originResponse);
      assert.equal(origin.mock.calls.at(-1).arguments[0], request);
      assert.equal(request.bodyUsed, false);
      assert.equal(await request.text(), "preserve this body");
    }
    assert.equal(state.calls.length, 0);
    assert.equal(origin.mock.callCount(), 6);
  });
});

test("configured nested mount and root site respect segment boundaries", async () => {
  const nested = makeAssets(200, "/news/onionflash");
  await withFetch(async () => new Response("origin"), async origin => {
    const sibling = new Request("https://example.com/news/onionflash-other");
    await worker.fetch(sibling, nested.env);
    assert.equal(origin.mock.calls[0].arguments[0], sibling);
    const within = new Request("https://example.com/news/onionflash/feed.xml");
    await worker.fetch(within, nested.env);
    assert.equal(nested.calls[0], within);
  });
  const root = makeAssets(200, "");
  await withFetch(noOrigin, async origin => {
    const request = new Request("https://example.com/feed.xml");
    assert.equal((await worker.fetch(request, root.env)).status, 200);
    assert.equal(root.calls[0], request);
    assert.equal(origin.mock.callCount(), 0);
  });
});
