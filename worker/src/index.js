export default {
  async fetch(request) {
    if (request.method !== "POST") {
      return new Response("method", { status: 405 });
    }

    const url = await request.text();
    if (!url) return new Response("url", { status: 400 });

    const started = performance.now();

    try {
      const response = await fetch(url);
      if (response.ok && response.body) {
        const cacheUrl = new URL(request.url);
        cacheUrl.pathname = `/__benchmark_sink/${Date.now()}-${Math.random()}`;
        cacheUrl.search = "";
        const cacheRequest = new Request(cacheUrl.toString(), { method: "GET" });
        const cacheResponse = new Response(response.body, response);
        cacheResponse.headers.set("cache-control", "public, max-age=1");
        await caches.default.put(cacheRequest, cacheResponse);
      }

      const downloadMs = Math.round((performance.now() - started) * 100) / 100;
      return new Response(`${response.ok ? 1 : 0},${response.status},${downloadMs}`, {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    } catch (error) {
      return new Response("0,0,0", {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }
  },
};
