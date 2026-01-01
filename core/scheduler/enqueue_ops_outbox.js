#!/usr/bin/env node
/**
 * enqueue_ops_outbox.js: Enqueues daily ops messages into a JSONL outbox
 * with deduplication and deterministic fingerprinting.
 */
import fs from 'fs';
import path from 'path';
import 'dotenv/config';
import crypto from 'crypto';
import { execSync } from 'child_process';

const MESSAGE_DIR = 'ops/messages';
const OUTBOX_DIR = 'ops/outbox';
const OUTBOX_FILE = path.join(OUTBOX_DIR, 'outbox.jsonl');
const INDEX_FILE = path.join(OUTBOX_DIR, 'sent_index.json');

/**
 * Returns YYYYMMDD for a Date object
 */
function formatDate(date) {
  return date.toISOString().split('T')[0].replace(/-/g, '');
}

/**
 * SHA256 first 8 characters from raw string content
 */
function getHash8(content) {
  return crypto.createHash('sha256').update(content).digest('hex').substring(0, 8);
}

/**
 * Ensures directories and outbox file exist
 */
function ensureBase() {
  if (!fs.existsSync(OUTBOX_DIR)) fs.mkdirSync(OUTBOX_DIR, { recursive: true });
  if (!fs.existsSync(OUTBOX_FILE)) fs.writeFileSync(OUTBOX_FILE, '');
}

/**
 * Safe JSON parse that records errors to stderr
 */
function safeJSON(content, context) {
  try {
    return JSON.parse(content);
  } catch (e) {
    console.error(`[Error] Failed to parse ${context}: ${e.message}`);
    return null;
  }
}

/**
 * Index recovery: Scans outbox.jsonl to rebuild the ID set
 */
function recoverIndex(reason) {
  console.error(`[Outbox] Index recovery (Reason: ${reason})`);
  const idSet = new Set();
  
  if (fs.existsSync(INDEX_FILE)) {
    try {
      const badPath = `${INDEX_FILE}.bad.${Date.now()}`;
      fs.renameSync(INDEX_FILE, badPath);
      console.error(`[Outbox] Corrupt index moved to ${badPath}`);
    } catch (e) {}
  }

  if (fs.existsSync(OUTBOX_FILE)) {
    try {
      const content = fs.readFileSync(OUTBOX_FILE, 'utf8').trim();
      if (content) {
        content.split('\n').forEach((line, i) => {
          if (!line.trim()) return;
          const envelope = safeJSON(line, `outbox.jsonl line ${i+1}`);
          if (envelope && envelope.id) idSet.add(envelope.id);
        });
      }
    } catch (e) {
      console.error(`[Outbox] Critical failure during outbox scan: ${e.message}`);
    }
  }
  return idSet;
}

/**
 * Load index or recover
 */
function loadIndex() {
  if (!fs.existsSync(INDEX_FILE)) return recoverIndex('Missing index');
  
  const content = fs.readFileSync(INDEX_FILE, 'utf8');
  const index = safeJSON(content, 'sent_index.json');
  
  if (!index || !Array.isArray(index.ids)) {
    return recoverIndex('Invalid index format');
  }
  
  return new Set(index.ids);
}

/**
 * Save index deterministically (Sorted ids)
 */
function saveIndex(idSet) {
  try {
    const data = {
      version: 1,
      ids: Array.from(idSet).sort(),
      updated_at: new Date().toISOString()
    };
    fs.writeFileSync(INDEX_FILE, JSON.stringify(data, null, 2));
  } catch (e) {
    console.error(`[Error] Failed to save index: ${e.message}`);
  }
}

/**
 * Processes a single date for enqueueing
 */
function processDate(dateString, idSet) {
  const messagePath = path.join(MESSAGE_DIR, `message_${dateString}.json`);

  // 1. Fallback generation
  if (!fs.existsSync(messagePath)) {
    try {
      execSync(`node scheduler/generate_ops_message.js --date ${dateString}`, { stdio: 'inherit' });
    } catch (e) {
      // Logic continues; next check will catch the failure
    }
  }

  if (!fs.existsSync(messagePath)) {
    console.log(`[Outbox] [${dateString}] Missing message, skipped`);
    return;
  }

  // 2. Deterministic ID from RAW file bytes
  try {
    const rawContent = fs.readFileSync(messagePath, 'utf8');
    const hash = getHash8(rawContent);
    const msgId = `opsmsg-${dateString}-${hash}`;

    // 3. Deduplication
    if (idSet.has(msgId)) {
      console.log(`[Outbox] [${dateString}] Skip duplicate: ${msgId}`);
      return;
    }

    // 4. Verification of JSON validaty before enqueuing
    const payload = safeJSON(rawContent, messagePath);
    if (!payload) {
      console.error(`[Outbox] [${dateString}] Invalid JSON payload, skipped`);
      return;
    }

    // 5. Envelope and Append (Atomic-ish)
    const envelope = {
      id: msgId,
      date: dateString,
      created_at: new Date().toISOString(),
      channel: 'ops',
      payload: payload
    };

    fs.appendFileSync(OUTBOX_FILE, JSON.stringify(envelope) + '\n');
    idSet.add(msgId);
    console.log(`[Outbox] [${dateString}] Enqueued: ${msgId}`);

  } catch (e) {
    console.error(`[Error] [${dateString}] unexpected: ${e.message}`);
  }
}

/**
 * Main entry
 */
function main() {
  try {
    ensureBase();
    const idSet = loadIndex();

    const args = process.argv.slice(2);
    let opts = { date: null, last: null };
    for (let i = 0; i < args.length; i++) {
      if (args[i] === '--date') opts.date = args[++i];
      else if (args[i] === '--last') opts.last = parseInt(args[++i], 10);
    }

    const yesterday = new Date();
    yesterday.setUTCDate(yesterday.getUTCDate() - 1);
    const yesterdayStr = formatDate(yesterday);

    let dates = [];

    // Precedence: --date > --last > default
    if (opts.date) {
      dates = [opts.date];
    } else if (opts.last && opts.last > 0) {
      for (let i = 0; i < opts.last; i++) {
        const d = new Date(yesterday.getTime());
        d.setUTCDate(d.getUTCDate() - i);
        dates.push(formatDate(d));
      }
      dates.reverse(); // ASC: [oldest .. yesterday]
    } else {
      dates = [yesterdayStr];
    }

    // One-line debug before processing
    console.error(`[Outbox] Dates: ${dates.join(', ')}`);

    dates.forEach(d => processDate(d, idSet));
    
    // Always save index once at the end (persists recovery or new items)
    saveIndex(idSet);

  } catch (e) {
    console.error(`[Fatal Error] ${e.message}`);
  }
  process.exit(0);
}

main();
