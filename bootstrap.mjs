export default {
  fetch() {
    return new Response("Onion Flash is preparing its first publication.\n", {
      status: 503,
      headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" }
    });
  }
};
