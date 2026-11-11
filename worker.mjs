// Every public file but the article permalinks stays on Cloudflare. Permalinks resolve
// through the immutable manifest selected by this Worker version, so rollbacks stay exact.
const HASH = /^[a-f0-9]{64}$/;
const HTML_POLICY = "default-src 'none'; script-src 'none'; style-src 'self'; img-src 'self'; connect-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const DATA_POLICY = "default-src 'none'; script-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
// TTL matches the application's hot pages; MANIFEST_BYTES matches archive_store.MAX_MANIFEST_BYTES.
const TTL = 60, OBJECT_TTL = 31536000, MANIFEST_BYTES = 8 * 1024 * 1024;
const TIMEOUT_MS = 5000, ATTEMPTS = 2, BACKOFF_MS = 200, BACKOFF_CAP_MS = 1000;

function archivePath(path) {
  const match = path.match(/^\/news\/([A-Za-z0-9_-]{1,64})(\/|\/index\.html|\/story\.json)?$/);
  if (!match) return null;
  if (!match[2] || match[2] === "/index.html") return { redirect: `/news/${match[1]}/` };
  return { key: `news/${match[1]}/${match[2] === "/story.json" ? "story.json" : "index.html"}` };
}

function publicHeaders(key, sha, size) {
  const html = key.endsWith(".html");
  const headers = new Headers({
    "Content-Type": html ? "text/html; charset=utf-8" : "application/json; charset=utf-8",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Content-Security-Policy": html ? HTML_POLICY : DATA_POLICY,
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
    "Cache-Control": `public, max-age=${TTL}, must-revalidate`,
    "ETag": `"${sha}"`,
  });
  if (size !== undefined) headers.set("Content-Length", String(size));
  return headers;
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
      // Workers rejects redirect: "error". With "manual" a 3xx comes back, and consume refuses it as not ok.
      response = await fetch(url, { method, redirect: "manual", signal: controller.signal,
        // Do not forward cookies, Authorization, Host, Range or arbitrary headers.
        headers: { "Accept-Encoding": "identity" },
        cf: { cacheEverything: true, cacheTtlByStatus: { "200-299": OBJECT_TTL, "403-404": 0, "500-599": 0 } },
      });
      // consume reads any body within this attempt's timeout and returns refusals rather than
      // throwing, so only failed fetches or reads, 429 and 5xx are retried.
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

// The body if it has at most limit bytes and the expected SHA-256, otherwise null.
async function verifiedBytes(response, limit, hash) {
  const reader = response.body?.getReader();
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
    await reader.cancel();
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return await sha256(bytes) === hash ? bytes : null;
}

async function manifest(origin, hash) {
  if (!HASH.test(hash || "")) throw new Error("Invalid manifest identity");
  // A refused manifest is null, which the root and shard checks reject.
  return originRequest(new URL(`manifests/${hash}.json`, origin), "GET", async response => {
    if (!response.ok) {
      await response.body?.cancel();
      return null;
    }
    const bytes = await verifiedBytes(response, MANIFEST_BYTES, hash);
    return bytes && JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  });
}

async function archived(request, env, key) {
  // The publisher validated the origin before binding it to this version.
  const origin = new URL(env.ARCHIVE_ORIGIN);
  const root = await manifest(origin, env.ARCHIVE_MANIFEST_SHA);
  if (root?.schema !== 1 || !Number.isInteger(root.shard_chars) || root.shard_chars < 1 || root.shard_chars > 4 || !root.shards || typeof root.shards !== "object" || Array.isArray(root.shards)) throw new Error("Invalid archive root");
  const shardHash = root.shards[(await sha256(new TextEncoder().encode(key))).slice(0, root.shard_chars)];
  if (shardHash === undefined) return error(404);
  const shard = await manifest(origin, shardHash);
  if (shard?.schema !== 1 || !shard.entries || typeof shard.entries !== "object" || Array.isArray(shard.entries)) throw new Error("Invalid archive shard");
  const entry = shard.entries[key];
  if (entry === undefined) return error(404);
  if (!entry || !HASH.test(entry.sha256 || "") || !Number.isSafeInteger(entry.size) || entry.size < 0) throw new Error("Invalid archive entry");
  const headers = publicHeaders(key, entry.sha256, entry.size);
  if (request.headers.get("if-none-match") === headers.get("etag")) {
    headers.delete("Content-Length");
    return new Response(null, { status: 304, headers });
  }
  return originRequest(new URL(`objects/${entry.sha256}`, origin), request.method, async response => {
    if (!response.ok || Number(response.headers.get("content-length")) !== entry.size || response.headers.get("x-amz-meta-sha256") !== entry.sha256) {
      await response.body?.cancel();
      return error(response.status === 403 || response.status === 404 ? 404 : 503);
    }
    if (request.method === "HEAD") return new Response(null, { status: 200, headers });
    // Serve only the bytes the manifest names, so an object replaced in storage fails closed.
    const bytes = await verifiedBytes(response, entry.size, entry.sha256);
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
