const CACHE_NAME = "agy-pwa-shell-v10";
const SHELL_ASSETS = [
  "./",
  "./static/styles.css",
  "./static/app.js",
  "./static/core.js",
  "./static/chat_view.js",
  "./static/markdown.js",
  "./manifest.webmanifest",
  "./icons/icon-180.png",
  "./icons/icon-192.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches
      .open(CACHE_NAME)
      .then((cache) => cache.addAll(SHELL_ASSETS).catch(() => {}))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
      )
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;
  if (
    url.pathname.includes("/api/") ||
    url.pathname.endsWith("/healthz") ||
    url.pathname.endsWith("/ca.crt")
  ) {
    return;
  }

  event.respondWith(
    fetch(req)
      .then((resp) => {
        if (resp && resp.ok && resp.type === "basic") {
          const clone = resp.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(req, clone)).catch(() => {});
        }
        return resp;
      })
      .catch(async (err) => {
        const cached = await caches.match(req);
        if (cached) return cached;
        throw err;
      })
  );
});

self.addEventListener("push", (event) => {
  let data = {
    title: "AGY Tasks",
    body: "New update waiting.",
    url: "./",
    tag: "agy-default",
    thread_id: null,
    unread_total: 0,
  };
  try {
    if (event.data) {
      data = { ...data, ...event.data.json() };
    }
  } catch {
    const txt = event.data ? event.data.text() : "";
    if (txt) data.body = txt;
  }

  const badgeTask =
    self.navigator && typeof self.navigator.setAppBadge === "function"
      ? self.navigator.setAppBadge(Number(data.unread_total || 1)).catch(() => {})
      : Promise.resolve();

  const notifyTask = self.registration.showNotification(data.title || "AGY Tasks", {
    body: data.body || "",
    icon: "./icons/icon-192.png",
    badge: "./icons/icon-180.png",
    tag: data.tag || "agy-default",
    renotify: true,
    data: { url: data.url || "./", thread_id: data.thread_id || null },
  });

  event.waitUntil(Promise.all([badgeTask, notifyTask]));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const payload = event.notification.data || {};
  const targetUrl = payload.url || "./";
  const threadId = payload.thread_id || null;

  event.waitUntil(
    self.clients
      .matchAll({ type: "window", includeUncontrolled: true })
      .then((clientsList) => {
        for (const client of clientsList) {
          if ("focus" in client) {
            client.focus();
            if (threadId) {
              client.postMessage({ type: "OPEN_THREAD", thread_id: threadId });
            }
            return;
          }
        }
        if (self.clients.openWindow) {
          return self.clients.openWindow(targetUrl);
        }
        return undefined;
      })
  );
});
