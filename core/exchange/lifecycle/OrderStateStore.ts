/**
 * Order State Store
 *
 * Persists order lifecycle entries to filesystem for durability.
 * Uses JSON files with atomic writes (write to temp, rename).
 */

import fs from "node:fs/promises";
import path from "node:path";
import { OrderLifecycleEntry } from "./order_states.js";

const DEFAULT_STORE_DIR = "/opt/quantlab/bridge/orders";

export class OrderStateStore {
    readonly #storeDir: string;
    readonly #cache: Map<string, OrderLifecycleEntry>;

    constructor(storeDir: string = DEFAULT_STORE_DIR) {
        this.#storeDir = storeDir;
        this.#cache = new Map();
    }

    /**
     * Initialize the store - create directory if needed.
     */
    async init(): Promise<void> {
        await fs.mkdir(this.#storeDir, { recursive: true });
        await this.loadExisting();
    }

    /**
     * Load existing entries from disk into cache.
     */
    private async loadExisting(): Promise<void> {
        try {
            const files = await fs.readdir(this.#storeDir);
            const jsonFiles = files.filter(f => f.endsWith(".json"));

            for (const file of jsonFiles) {
                try {
                    const content = await fs.readFile(
                        path.join(this.#storeDir, file),
                        "utf-8"
                    );
                    const entry = JSON.parse(content) as OrderLifecycleEntry;
                    this.#cache.set(entry.bridgeId, entry);
                } catch {
                    // Skip corrupted files
                }
            }
        } catch {
            // Directory might not exist yet
        }
    }

    /**
     * Save an entry to disk and cache.
     */
    async save(entry: OrderLifecycleEntry): Promise<void> {
        const filePath = this.getFilePath(entry.bridgeId);
        const tempPath = `${filePath}.tmp`;

        // Atomic write: write to temp, then rename
        const content = JSON.stringify(entry, null, 2);
        await fs.writeFile(tempPath, content, "utf-8");
        await fs.rename(tempPath, filePath);

        // Update cache
        this.#cache.set(entry.bridgeId, entry);
    }

    /**
     * Get an entry by bridge ID.
     */
    async get(bridgeId: string): Promise<OrderLifecycleEntry | null> {
        // Check cache first
        const cached = this.#cache.get(bridgeId);
        if (cached) {
            return cached;
        }

        // Try to load from disk
        try {
            const content = await fs.readFile(this.getFilePath(bridgeId), "utf-8");
            const entry = JSON.parse(content) as OrderLifecycleEntry;
            this.#cache.set(bridgeId, entry);
            return entry;
        } catch {
            return null;
        }
    }

    /**
     * Delete an entry.
     */
    async delete(bridgeId: string): Promise<boolean> {
        this.#cache.delete(bridgeId);

        try {
            await fs.unlink(this.getFilePath(bridgeId));
            return true;
        } catch {
            return false;
        }
    }

    /**
     * Get all entries (from cache).
     */
    getAll(): OrderLifecycleEntry[] {
        return Array.from(this.#cache.values());
    }

    /**
     * Get entries by state.
     */
    getByState(state: string): OrderLifecycleEntry[] {
        return this.getAll().filter(e => e.state === state);
    }

    /**
     * Get entries by symbol.
     */
    getBySymbol(symbol: string): OrderLifecycleEntry[] {
        return this.getAll().filter(e => e.symbol === symbol);
    }

    /**
     * Get entries created within a time range.
     */
    getByTimeRange(startMs: number, endMs: number): OrderLifecycleEntry[] {
        return this.getAll().filter(
            e => e.createdAt >= startMs && e.createdAt <= endMs
        );
    }

    /**
     * Get count of entries in each state.
     */
    getStateCounts(): Record<string, number> {
        const counts: Record<string, number> = {};

        for (const entry of this.#cache.values()) {
            counts[entry.state] = (counts[entry.state] || 0) + 1;
        }

        return counts;
    }

    /**
     * Cleanup old terminal entries (older than specified days).
     */
    async cleanupOldEntries(olderThanDays: number): Promise<number> {
        const cutoff = Date.now() - olderThanDays * 24 * 60 * 60 * 1000;
        const terminalStates = ["FILLED", "CANCELLED", "REJECTED", "FAILED", "EXPIRED"];
        let deletedCount = 0;

        for (const entry of this.#cache.values()) {
            if (terminalStates.includes(entry.state) && entry.updatedAt < cutoff) {
                await this.delete(entry.bridgeId);
                deletedCount++;
            }
        }

        return deletedCount;
    }

    private getFilePath(bridgeId: string): string {
        return path.join(this.#storeDir, `${bridgeId}.json`);
    }
}
