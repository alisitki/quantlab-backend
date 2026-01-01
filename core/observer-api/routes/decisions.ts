import { Router } from 'express';
import { readJSONL } from '../readers/JSONLReader.js';
import { DATA_PATHS } from '../config.js';
import path from 'path';

const router = Router();

function getDayRange(dateStr: string) {
    const y = parseInt(dateStr.slice(0, 4));
    const m = parseInt(dateStr.slice(4, 6)) - 1;
    const d = parseInt(dateStr.slice(6, 8));

    const start = new Date(Date.UTC(y, m, d, 0, 0, 0, 0)).getTime();
    const end = new Date(Date.UTC(y, m, d, 23, 59, 59, 999)).getTime();
    return { start, end };
}

// 5) GET /decisions?date=YYYYMMDD
router.get('/', async (req, res) => {
    const date = req.query.date as string;
    if (!date || !/^\d{8}$/.test(date)) {
        return res.status(400).json({ error: 'Valid date parameter YYYYMMDD is required' });
    }

    const { start, end } = getDayRange(date);
    const eventsLog = path.join(DATA_PATHS.logs, 'events.jsonl');

    const events = await readJSONL(eventsLog, {
        filter: (e) => e.evaluated_at >= start && e.evaluated_at <= end
    });

    const decisionsMap = new Map<string, any>();

    events.forEach(e => {
        const id = e.decision_id || e.client_order_id || 'unknown';
        if (!decisionsMap.has(id)) {
            decisionsMap.set(id, {
                decision_id: id,
                symbol: e.symbol || 'N/A',
                first_seen_at: e.evaluated_at,
                final_outcome: e.outcome,
                policy_snapshot_hash: e.policy_snapshot_hash || 'N/A'
            });
        } else {
            // Update with later outcome if applicable
            const d = decisionsMap.get(id);
            if (e.evaluated_at > d.first_seen_at) {
                d.final_outcome = e.outcome;
            }
            if (e.evaluated_at < d.first_seen_at) {
                d.first_seen_at = e.evaluated_at;
            }
        }
    });

    res.json(Array.from(decisionsMap.values()));
});

// 6) GET /decisions/:id/trace
router.get('/:id/trace', async (req, res) => {
    const id = req.params.id;
    const eventsLog = path.join(DATA_PATHS.logs, 'events.jsonl');

    const events = await readJSONL(eventsLog, {
        filter: (e) => e.decision_id === id || e.client_order_id === id
    });

    if (events.length === 0) {
        return res.status(404).json({ error: `No trace found for decision ${id}` });
    }

    // Deterministic sort by timestamp
    const sorted = events.sort((a, b) => a.evaluated_at - b.evaluated_at);
    res.json(sorted);
});

export default router;
