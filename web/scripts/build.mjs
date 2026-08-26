import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "..", "..");
const out = resolve(root, "internal", "api", "static");

await mkdir(out, { recursive: true });
await writeFile(resolve(out, "index.html"), await readFile(resolve(root, "web", "src", "index.html"), "utf8"));
await writeFile(resolve(out, "app.js"), await readFile(resolve(root, "web", "src", "main.ts"), "utf8"));
await writeFile(resolve(out, "styles.css"), await readFile(resolve(root, "web", "src", "styles.css"), "utf8"));
