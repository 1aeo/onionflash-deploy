import { createHash } from "node:crypto";
import { mock } from "node:test";
import { gzipSync } from "node:zlib";

export const ORIGIN = "https://public.example/onionflash/public/";
export const TYPES = {
  "news/a/index.html": "text/html; charset=utf-8",
  "news/a/story.json": "application/json; charset=utf-8",
};

let fixtureSeq = 0;

export const sha256Hex = value => createHash("sha256").update(value).digest("hex");

export function makeAssets(status = 200, basePath = "/onionflash") {
  const calls = [];
  const ASSETS = {
    async fetch(request) {
      calls.push(request);
      return new Response(status === 404 ? "not found" : "static content", { status });
    },
  };
  return { calls, env: { BASE_PATH: basePath, ASSETS } };
}

export async function withFetch(responder, run) {
  const mocked = mock.method(globalThis, "fetch", responder);
  try {
    return await run(mocked);
  } finally {
    mocked.mock.restore();
  }
}

export function archiveFixture(contents = {}) {
  const objects = new Map();
  const entries = {};
  const seedKey = `news/seed-${++fixtureSeq}/story.json`;
  for (const key of [...Object.keys(TYPES), seedKey]) {
    const body = new TextEncoder().encode(contents[key] ?? `public content for ${key}`);
    const sha256 = sha256Hex(body);
    entries[key] = { key: `objects/${sha256}`, sha256, size: body.length, content_type: TYPES[key] ?? "application/json; charset=utf-8" };
    objects.set(`${ORIGIN}objects/${sha256}`, { body, sha256 });
  }
  const manifestRaw = JSON.stringify({ format: "onionflash-archive-manifest-v1", entries });
  const manifestBody = new Uint8Array(gzipSync(Buffer.from(manifestRaw, "utf8"), { mtime: 0 }));
  const manifestSha = sha256Hex(manifestBody);
  objects.set(`${ORIGIN}manifests/${manifestSha}.json.gz`, { body: manifestBody, sha256: manifestSha });
  const { calls: assetCalls, env: baseEnv } = makeAssets(404);
  const env = { ...baseEnv, ARCHIVE_ORIGIN: ORIGIN, ARCHIVE_MANIFEST_SHA: manifestSha };
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
  return { env, objects, manifestSha, entries, calls, assetCalls, respond };
}

export const withOrigin = (state, run, override) => withFetch(override || state.respond, () => run());
