// Hot assets stay on Cloudflare. Permanent pages/feeds resolve through the
// immutable manifest selected by this Worker version, so rollbacks stay exact.
const HASH = /^[a-f0-9]{64}$/;
const TOPICS = "security|release|dos|directory-authority|relay|protocol|feature|network";
const HTML_POLICY = "default-src 'none'; script-src 'none'; style-src 'self'; img-src 'self'; connect-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const DATA_POLICY = "default-src 'none'; script-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

function integer(env, name, fallback, min, max) {
  const raw = env[name] ?? fallback;
  if (!/^\d+$/.test(String(raw))) throw new Error("Invalid archive limit");
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error("Invalid archive limit");
  return value;
}

function topicAllowed(topic, env) {
  const configured = env.ARCHIVE_TOPICS ?? TOPICS.split("|").join(",");
  if (typeof configured !== "string" || configured.length > 4096) return false;
  const topics = configured.split(",");
  if (topics.length > 100 || topics.some(value => !/^[a-z][a-z0-9-]{0,39}$/.test(value))) return false;
  return topics.includes(topic);
}

function archivePath(path, env) {
  if (path === "/feed.xml" || path === "/feed.json") return { key: path.slice(1) };
  const topic = path.match(/^\/tags\/([a-z][a-z0-9-]{0,39})\.xml$/);
  if (topic) return topicAllowed(topic[1], env) ? { key: path.slice(1) } : null;
  let match = path.match(/^\/topics\/([a-z][a-z0-9-]{0,39})(\/|\/index\.html)?$/);
  if (match) {
    if (!topicAllowed(match[1], env)) return null;
    if (match[2] !== "/") return { redirect: `/topics/${match[1]}/` };
    return { key: `topics/${match[1]}/index.html` };
  }
  match = path.match(/^\/topics\/([a-z][a-z0-9-]{0,39})\/page\/([1-9][0-9]{0,8})(\/|\/index\.html)?$/);
  if (match) {
    if (!topicAllowed(match[1], env)) return null;
    if (match[2] === "1") return { redirect: `/topics/${match[1]}/` };
    if (match[3] !== "/") return { redirect: `/topics/${match[1]}/page/${match[2]}/` };
    return { key: `topics/${match[1]}/page/${match[2]}/index.html` };
  }
  match = path.match(/^\/news\/([A-Za-z0-9_-]{1,64})(\/|\/index\.html|\/story\.json)?$/);
  if (match) {
    if (!match[2] || match[2] === "/index.html") return { redirect: `/news/${match[1]}/` };
    return { key: `news/${match[1]}/${match[2] === "/story.json" ? "story.json" : "index.html"}` };
  }
  match = path.match(/^\/page\/([1-9][0-9]{0,8})(\/|\/index\.html)?$/);
  if (match) {
    if (match[1] === "1") return { redirect: "/" };
    if (match[2] !== "/") return { redirect: `/page/${match[1]}/` };
    return { key: `page/${match[1]}/index.html` };
  }
  return null;
}

function mime(key) {
  if (key.endsWith(".html")) return "text/html; charset=utf-8";
  // Like the static site, RSS feeds keep the generic XML type, which browsers display instead of downloading.
  if (key.endsWith(".xml")) return "application/xml; charset=utf-8";
  return key === "feed.json" ? "application/feed+json; charset=utf-8" : "application/json; charset=utf-8";
}

function publicHeaders(key, ttl, sha, size) {
  const type = mime(key);
  const headers = new Headers({
    "Content-Type": type,
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    // The browser's XML viewer styles feeds with inline CSS and data: icons.
    "Content-Security-Policy": key.endsWith(".html") ? HTML_POLICY : DATA_POLICY + (key.endsWith(".xml") ? "; style-src 'unsafe-inline'; img-src data:" : ""),
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
    "Cache-Control": `public, max-age=${ttl}, must-revalidate`,
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

async function originRequest(url, method, settings, consume) {
  for (let attempt = 0; attempt < settings.attempts; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), settings.timeout);
    let response;
    try {
      // Workers rejects redirect: "error". With "manual" a 3xx comes back, and consume refuses it as not ok.
      response = await fetch(url, { method, redirect: "manual", signal: controller.signal,
        // Do not forward cookies, Authorization, Host, Range or arbitrary headers.
        headers: { "Accept-Encoding": "identity" },
        cf: { cacheEverything: true, cacheTtlByStatus: { "200-299": settings.objectTtl, "403-404": 0, "500-599": 0 } },
      });
      // consume reads any body within this attempt's timeout and returns refusals rather than
      // throwing, so only failed fetches or reads, 429 and 5xx are retried.
      if (response.status !== 429 && response.status < 500) return await consume(response);
      await response.body?.cancel();
    } catch (failure) {
      if (attempt + 1 === settings.attempts) throw failure;
    } finally {
      clearTimeout(timer);
    }
    if (attempt + 1 < settings.attempts) {
      const retryAfter = Number(response?.headers.get("retry-after"));
      const wait = Math.min(settings.cap, Math.max(settings.backoff * 2 ** attempt, Number.isFinite(retryAfter) ? retryAfter * 1000 : 0));
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

async function manifest(origin, hash, settings) {
  if (!HASH.test(hash || "")) throw new Error("Invalid manifest identity");
  // A refused manifest is null, which the root and shard checks reject.
  return originRequest(new URL(`manifests/${hash}.json`, origin), "GET", settings, async response => {
    if (!response.ok) {
      await response.body?.cancel();
      return null;
    }
    const bytes = await verifiedBytes(response, settings.manifestBytes, hash);
    return bytes && JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  });
}

async function archived(request, env, key) {
  const origin = new URL(env.ARCHIVE_ORIGIN);
  if (origin.protocol !== "https:" || origin.username || origin.password || origin.search || origin.hash || !/^\/[A-Za-z0-9_-]+\/public\/$/.test(origin.pathname)) throw new Error("Invalid archive origin");
  const settings = {
    ttl: integer(env, "ARCHIVE_CACHE_TTL", 60, 0, 86400),
    objectTtl: integer(env, "ARCHIVE_OBJECT_CACHE_TTL", 31536000, 60, 31536000),
    manifestBytes: integer(env, "ARCHIVE_MAX_MANIFEST_BYTES", 8388608, 1024, 16777216),
    timeout: integer(env, "ARCHIVE_TIMEOUT_MS", 5000, 100, 30000),
    attempts: integer(env, "ARCHIVE_RETRY_ATTEMPTS", 2, 1, 5),
    backoff: integer(env, "ARCHIVE_BACKOFF_MS", 200, 0, 5000),
    cap: integer(env, "ARCHIVE_BACKOFF_CAP_MS", 1000, 0, 10000),
  };
  if (settings.cap < settings.backoff) throw new Error("Invalid archive backoff");
  const root = await manifest(origin, env.ARCHIVE_MANIFEST_SHA, settings);
  if (root?.schema !== 1 || !Number.isInteger(root.shard_chars) || root.shard_chars < 1 || root.shard_chars > 4 || !root.shards || typeof root.shards !== "object" || Array.isArray(root.shards)) throw new Error("Invalid archive root");
  const shardHash = root.shards[(await sha256(new TextEncoder().encode(key))).slice(0, root.shard_chars)];
  if (shardHash === undefined) return error(404);
  const shard = await manifest(origin, shardHash, settings);
  if (shard?.schema !== 1 || !shard.entries || typeof shard.entries !== "object" || Array.isArray(shard.entries)) throw new Error("Invalid archive shard");
  const entry = shard.entries[key];
  if (entry === undefined) return error(404);
  // Headers follow the path, so roots that recorded another content type stay readable.
  if (!entry || !HASH.test(entry.sha256 || "") || !Number.isSafeInteger(entry.size) || entry.size < 0) throw new Error("Invalid archive entry");
  const headers = publicHeaders(key, settings.ttl, entry.sha256, entry.size);
  if (request.headers.get("if-none-match") === headers.get("etag")) {
    headers.delete("Content-Length");
    return new Response(null, { status: 304, headers });
  }
  return originRequest(new URL(`objects/${entry.sha256}`, origin), request.method, settings, async response => {
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
      const path = archivePath(url.pathname.slice(prefix.length), env);
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
