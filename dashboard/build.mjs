import { mkdir, copyFile } from "node:fs/promises";

await mkdir("dist/assets", { recursive: true });
await copyFile("src/index.html", "dist/index.html");
await copyFile("src/app.js", "dist/assets/dashboard.js");
