// Every public file but the article permalinks stays on Cloudflare. Permalinks resolve
// through the immutable manifest selected by this Worker version, so rollbacks stay exact.
const HASH = /^[a-f0-9]{64}$/;
const HTML_POLICY = "default-src 'none'; script-src 'none'; style-src 'self'; img-src 'self'; connect-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const DATA_POLICY = "default-src 'none'; script-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const TTL = 60, OBJECT_TTL = 31536000, MANIFEST_BYTES = 8 * 1024 * 1024, MANIFEST_RAW_BYTES = 16 * 1024 * 1024;
const TIMEOUT_MS = 5000, ATTEMPTS = 2, BACKOFF_MS = 200, BACKOFF_CAP_MS = 1000, CACHE_LIMIT = 4;
const MANIFEST_FORMAT = "onionflash-archive-manifest-v1";
const manifestCache = new Map();
const isRecord = value => Boolean(value) && typeof value === "object" && !Array.isArray(value);

function archivePath(path) {
  const match = path.match(/^\/news\/([A-Za-z0-9_-]{1,64})(\/|\/index\.html|\/story\.json)?$/);
  if (!match) return null;
  if (!match[2] || match[2] === "/index.html") return { redirect: `/news/${match[1]}/` };
  return { key: `news/${match[1]}/${match[2] === "/story.json" ? "story.json" : "index.html"}` };
}

function publicHeaders(key, sha, size) {
  const html = key.endsWith(".html");
  return new Headers({
    "Content-Type": html ? "text/html; charset=utf-8" : "application/json; charset=utf-8",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Content-Security-Policy": html ? HTML_POLICY : DATA_POLICY,
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
    "Cache-Control": `public, max-age=${TTL}, must-revalidate`,
    "ETag": `"${sha}"`,
    "Content-Length": String(size),
  });
}

function error(status) {
  const headers = { "Content-Type": "text/plain; charset=utf-8", "X-Content-Type-Options": "nosniff", "Content-Security-Policy": DATA_POLICY, "Cache-Control": "no-store" };
  if (status === 503) headers["Retry-After"] = "60";
  return new Response(status === 404 ? "Not found" : "Temporarily unavailable", { status, headers });
}

async function sha256(bytes) {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join("");
}

async function originRequest(url, method, consume) {
  for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
    let response;
    try {
      response = await fetch(url, {
        method, redirect: "manual", signal: controller.signal,
        headers: { "Accept-Encoding": "identity" },
        cf: { cacheEverything: true, cacheTtlByStatus: { "200-299": OBJECT_TTL, "403-404": 0, "500-599": 0 } },
      });
      if (response.status !== 429 && response.status < 500) return await consume(response);
      await response.body?.cancel();
    } catch (failure) {
      if (attempt + 1 === ATTEMPTS) throw failure;
    } finally {
      clearTimeout(timer);
    }
    if (attempt + 1 < ATTEMPTS) {
      const retryAfter = Number(response?.headers.get("retry-after"));
      const wait = Math.min(BACKOFF_CAP_MS, Math.max(BACKOFF_MS * 2 ** attempt, Number.isFinite(retryAfter) ? retryAfter * 1000 : 0));
      if (wait) await new Promise(resolve => setTimeout(resolve, wait));
    }
  }
  throw new Error("Archive request failed");
}

async function readLimited(stream, limit) {
  const reader = stream?.getReader();
  if (!reader) return null;
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > limit) return null;
      chunks.push(value);
    }
  } finally {
    await reader.cancel().catch(() => {});
  }
  return new Uint8Array(await new Blob(chunks).arrayBuffer());
}

async function verifiedBytes(response, limit, hash) {
  const bytes = await readLimited(response.body, limit);
  return bytes && await sha256(bytes) === hash ? bytes : null;
}

async function decompressGzip(bytes, limit) {
  const stream = new DecompressionStream("gzip");
  const writer = stream.writable.getWriter();
  let failed = false;
  const writeDone = writer.write(bytes).then(() => writer.close()).catch(() => { failed = true; });
  const out = await readLimited(stream.readable, limit);
  await writeDone;
  return failed ? null : out;
}

async function manifestEntries(origin, hash) {
  if (!HASH.test(hash || "")) throw new Error("Invalid manifest identity");
  const key = `${origin.href}|${hash}`;
  if (manifestCache.has(key)) return manifestCache.get(key);
  const gz = await originRequest(new URL(`manifests/${hash}.json.gz`, origin), "GET", async res =>
    res.ok ? verifiedBytes(res, MANIFEST_BYTES, hash) : (await res.body?.cancel(), null));
  const raw = gz && await decompressGzip(gz, MANIFEST_RAW_BYTES);
  if (!raw) throw new Error("Missing or invalid archive manifest");
  const parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
  if (!isRecord(parsed) || parsed.format !== MANIFEST_FORMAT || !isRecord(parsed.entries)) throw new Error("Invalid archive manifest");
  if (manifestCache.size >= CACHE_LIMIT) manifestCache.delete(manifestCache.keys().next().value);
  manifestCache.set(key, parsed.entries);
  return parsed.entries;
}

async function archived(request, env, key) {
  const origin = new URL(env.ARCHIVE_ORIGIN);
  const entries = await manifestEntries(origin, env.ARCHIVE_MANIFEST_SHA);
  if (!Object.hasOwn(entries, key)) return error(404);
  const entry = entries[key];
  if (!isRecord(entry) || !HASH.test(entry.sha256 || "") || entry.key !== `objects/${entry.sha256}` ||
      !Number.isSafeInteger(entry.size) || entry.size < 0 || typeof entry.content_type !== "string" || !entry.content_type) {
    throw new Error("Invalid archive entry");
  }
  const headers = publicHeaders(key, entry.sha256, entry.size);
  if (request.headers.get("if-none-match") === headers.get("etag")) {
    headers.delete("Content-Length");
    return new Response(null, { status: 304, headers });
  }
  return originRequest(new URL(entry.key, origin), request.method, async res => {
    if (!res.ok || Number(res.headers.get("content-length")) !== entry.size || res.headers.get("x-amz-meta-sha256") !== entry.sha256) {
      await res.body?.cancel();
      return error(res.status === 403 || res.status === 404 ? 404 : 503);
    }
    if (request.method === "HEAD") { await res.body?.cancel(); return new Response(null, { status: 200, headers }); }
    const bytes = await verifiedBytes(res, entry.size, entry.sha256);
    return bytes ? new Response(bytes, { status: 200, headers }) : error(503);
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const prefix = env.BASE_PATH || "";
    if (prefix && url.pathname !== prefix && !url.pathname.startsWith(prefix + "/")) return fetch(request);
    if (prefix && url.pathname === prefix) {
      url.pathname += "/";
      return Response.redirect(url.toString(), 308);
    }
    if (env.ARCHIVE_ORIGIN) {
      const path = archivePath(url.pathname.slice(prefix.length));
      if (path) {
        if (request.method !== "GET" && request.method !== "HEAD") return new Response("Method not allowed", { status: 405, headers: { Allow: "GET, HEAD" } });
        if (path.redirect) {
          url.pathname = prefix + path.redirect;
          return Response.redirect(url.toString(), 308);
        }
        try { return await archived(request, env, path.key); } catch { return error(503); }
      }
    }
    return env.ASSETS.fetch(request);
  },
};
