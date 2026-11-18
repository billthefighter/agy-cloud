import { appMode, boot } from "./core.js";
import { initChatMode } from "./chat_view.js";
import { initHealthMode } from "./health_view.js";

if (appMode === "health") {
  initHealthMode();
} else {
  initChatMode();
}

boot();
