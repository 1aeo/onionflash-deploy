// A separate alias Worker keeps the canonical site's static-asset routing intact.
export default {
  async fetch(request, env) {
    const incoming = new URL(request.url);
    const target = new URL(env.CANONICAL_URL);
    const mount = target.pathname.replace(/\/$/, "");
    // Set path/query on a fixed origin: //host paths must never become origins.
    target.pathname = incoming.pathname === mount
      ? mount + "/"
      : incoming.pathname.startsWith(mount + "/") ? incoming.pathname : mount + incoming.pathname;
    target.search = incoming.search;
    return Response.redirect(target.toString(), 308);
  },
};
