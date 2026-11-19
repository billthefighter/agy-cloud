import { boot, coreApi, toast, viewScript } from "./core.js";
import { initChatMode } from "./chat_view.js";

async function start() {
  if (viewScript) {
    try {
      const mod = await import(viewScript);
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
