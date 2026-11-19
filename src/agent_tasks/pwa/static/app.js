import { boot, coreApi, toast, viewScript } from "./core.js";
import { initChatMode } from "./chat_view.js";

async function start() {
  if (viewScript) {
    try {
      const resolvedUrl = new URL(viewScript, document.baseURI).href;
      const mod = await import(resolvedUrl);
      if (typeof mod.init === "function") {
        mod.init(coreApi);
      } else if (typeof mod.default === "function") {
        mod.default(coreApi);
      } else {
        throw new Error(`Plugin view module ${viewScript} does not export init(core).`);
      }
    } catch (err) {
      console.error("Failed to load plugin view module:", viewScript, err);
      toast(`Failed to load view module: ${err?.message || err}`);
      initChatMode();
    }
  } else {
    initChatMode();
  }
  await boot();
}

start();
