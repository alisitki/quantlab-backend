set -euo pipefail
node - <<"NODE"
import { readFileSync, existsSync } from "node:fs";
import path from "node:path";

const root = "evidence/sprint6-acceptance-full2day-20260117-20260118-20260209";

function collectStrings(obj, out){
  if (obj === null || obj === undefined) return;
  if (typeof obj === "string") { out.push(obj); return; }
  if (Array.isArray(obj)) { for (const v of obj) collectStrings(v, out); return; }
  if (typeof obj === "object") { for (const v of Object.values(obj)) collectStrings(v, out); }
}

const summary = JSON.parse(readFileSync(path.join(root, "summary.json"), "utf8"));
const s = [];
collectStrings(summary, s);
const summaryPaths = [...new Set(s.filter(x => x.includes("/") && (x.includes(".") || x.endsWith(".sh")) ))];

const readme = readFileSync(path.join(root, "README.md"), "utf8");
const mdPaths = [];
for (const m of readme.matchAll(/`([^`]+)`/g)) {
  const p = m[1];
  if (p.includes("/") && (p.includes(".") || p.endsWith("/"))) mdPaths.push(p);
}
const readmePaths = [...new Set(mdPaths.filter(p => !p.includes("*")))];

function check(list, label){
  const missing=[];
  for (const p of list) {
    const full = path.join(root, p);
    if (!existsSync(full)) missing.push(p);
  }
  console.log(`${label}_paths_count=${list.length}`);
  console.log(`${label}_missing_count=${missing.length}`);
  for (const m of missing) console.log(`${label}_MISSING ${m}`);
  return missing.length;
}

let miss = 0;
miss += check(summaryPaths, "summary");
miss += check(readmePaths, "readme");

if (miss === 0) {
  console.log("INTEGRITY_CHECK: PASS");
  process.exit(0);
}
console.log("INTEGRITY_CHECK: FAIL");
process.exit(2);
NODE
