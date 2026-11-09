// The /onionflash* route also matches sibling paths. Preserve those requests
// exactly, and only serve assets within the configured application boundary.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const prefix = env.BASE_PATH || "";
    if (prefix && url.pathname !== prefix && !url.pathname.startsWith(prefix + "/")) {
      return fetch(request);
    }
    if (prefix && url.pathname === prefix) {
      url.pathname += "/";
      return Response.redirect(url.toString(), 308);
    }
    // In-scope misses remain asset 404s; never fall back to the site's origin.
    return env.ASSETS.fetch(request);
  }
};
