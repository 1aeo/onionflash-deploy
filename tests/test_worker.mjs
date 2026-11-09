import assert from "node:assert/strict";
import { mock, test } from "node:test";
import worker from "../worker.mjs";

function assets(status = 200) {
  const calls = [];
  const binding = {
    async fetch(request) {
      calls.push(request);
      return new Response(status === 404 ? "not found" : "static content", { status });
    },
  };
  return { calls, env: { BASE_PATH: "/onionflash", ASSETS: binding } };
}

test("mount redirect preserves the full query string", async () => {
  const state = assets();
  const origin = mock.method(globalThis, "fetch", async () => {
    throw new Error("unexpected origin request");
  });
  try {
    for (const query of ["", "?ref=rss", "?q=a%2Fb&same=1&same=2", "?next=https%3A%2F%2Felsewhere.example%2F"]) {
      const response = await worker.fetch(new Request(`https://example.com/onionflash${query}`), state.env);
      assert.equal(response.status, 308);
      assert.equal(response.headers.get("location"), `https://example.com/onionflash/${query}`);
    }
    assert.equal(state.calls.length, 0);
    assert.equal(origin.mock.callCount(), 0);
  } finally {
    origin.mock.restore();
  }
});

test("application subtree uses only the static assets binding", async () => {
  const state = assets();
  const origin = mock.method(globalThis, "fetch", async () => {
    throw new Error("unexpected origin request");
  });
  try {
    for (const path of ["/onionflash/", "/onionflash/feed.xml?reader=1", "/onionflash/news/abc/", "/onionflash/tags/security.xml"]) {
      const request = new Request(`https://example.com${path}`);
      const response = await worker.fetch(request, state.env);
      assert.equal(response.status, 200);
      assert.equal(state.calls.at(-1), request);
    }
    assert.equal(state.calls.length, 4);
    assert.equal(origin.mock.callCount(), 0);
  } finally {
    origin.mock.restore();
  }
});

test("in-scope asset 404 does not fall back to the site's origin", async () => {
  const state = assets(404);
  const origin = mock.method(globalThis, "fetch", async () => new Response("origin must not be used"));
  try {
    const request = new Request("https://example.com/onionflash/not-found");
    const response = await worker.fetch(request, state.env);
    assert.equal(response.status, 404);
    assert.equal(await response.text(), "not found");
    assert.equal(state.calls[0], request);
    assert.equal(origin.mock.callCount(), 0);
  } finally {
    origin.mock.restore();
  }
});

test("similar and unrelated paths preserve the original request", async () => {
  const state = assets();
  const originResponse = new Response("origin content", { status: 202 });
  const origin = mock.method(globalThis, "fetch", async () => originResponse);
  try {
    for (const path of ["/onionflash-old", "/onionflashish/feed.xml", "/onionflash%2Ffeed.xml", "/ONIONFLASH/", "/other?source=onionflash", "/onionflash/../other"]) {
      const request = new Request(`https://example.com${path}`, {
        method: "POST",
        headers: { "X-Original-Header": "preserved" },
        body: "preserve this body",
      });
      const response = await worker.fetch(request, state.env);
      assert.equal(response, originResponse);
      assert.equal(origin.mock.calls.at(-1).arguments[0], request);
      assert.equal(request.bodyUsed, false);
      assert.equal(await request.text(), "preserve this body");
    }
    assert.equal(state.calls.length, 0);
    assert.equal(origin.mock.callCount(), 6);
  } finally {
    origin.mock.restore();
  }
});

test("configured nested mount respects segment boundaries", async () => {
  const state = assets();
  state.env.BASE_PATH = "/news/onionflash";
  const origin = mock.method(globalThis, "fetch", async () => new Response("origin"));
  try {
    const sibling = new Request("https://example.com/news/onionflash-other");
    await worker.fetch(sibling, state.env);
    assert.equal(origin.mock.calls[0].arguments[0], sibling);
    const within = new Request("https://example.com/news/onionflash/feed.xml");
    await worker.fetch(within, state.env);
    assert.equal(state.calls[0], within);
  } finally {
    origin.mock.restore();
  }
});

test("an explicitly configured root site serves its assets", async () => {
  const state = assets();
  state.env.BASE_PATH = "";
  const origin = mock.method(globalThis, "fetch", async () => {
    throw new Error("unexpected origin request");
  });
  try {
    const request = new Request("https://example.com/feed.xml");
    const response = await worker.fetch(request, state.env);
    assert.equal(response.status, 200);
    assert.equal(state.calls[0], request);
    assert.equal(origin.mock.callCount(), 0);
  } finally {
    origin.mock.restore();
  }
});
