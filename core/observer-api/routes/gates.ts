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

// 3) GET /gates/funnel?date=YYYYMMDD
router.get('/funnel', async (req, res) => {
    const date = req.query.date as string;
    if (!date || !/^\d{8}$/.test(date)) {
        return res.status(400).json({ error: 'Valid date parameter YYYYMMDD is required' });
    }

    const { start, end } = getDayRange(date);
    const eventsLog = path.join(DATA_PATHS.logs, 'events.jsonl');

    const events = await readJSONL(eventsLog, {
        filter: (e) => e.evaluated_at >= start && e.evaluated_at <= end
    });

    if (events.length === 0) {
        return res.status(404).json({ error: `No events found for ${date}` });
    }

    const funnel: Record<string, any> = {};
    const stages = [
        'EXECUTION_EVALUATED',
        'FUTURES_CANARY_EVALUATED',
        'FUTURES_RISK_EVALUATED',
        'FUTURES_FUNDING_EVALUATED',
        'FUTURES_ORDER_INTENT_MAPPED'
    ];

    stages.forEach(stage => {
        const stageEvents = events.filter(e => e.event_type === stage);
        funnel[stage] = {
            total: stageEvents.length,
            pass: stageEvents.filter(e => e.outcome === 'PASSED' || e.outcome === 'MAPPED').length,
            reject: stageEvents.filter(e => e.outcome === 'REJECTED').length,
            skip: stageEvents.filter(e => e.outcome === 'SKIPPED').length
        };
    });

    res.json(funnel);
});

// 4) GET /gates/reasons?date=YYYYMMDD
router.get('/reasons', async (req, res) => {
    const date = req.query.date as string;
    if (!date || !/^\d{8}$/.test(date)) {
        return res.status(400).json({ error: 'Valid date parameter YYYYMMDD is required' });
    }

    const { start, end } = getDayRange(date);
    const eventsLog = path.join(DATA_PATHS.logs, 'events.jsonl');

    const events = await readJSONL(eventsLog, {
        filter: (e) => e.evaluated_at >= start && e.evaluated_at <= end
    });

    const breakdown: Record<string, Record<string, number>> = {};

    events.forEach(e => {
        if (!breakdown[e.event_type]) breakdown[e.event_type] = {};
        const reason = e.reason_code || 'UNKNOWN';
        breakdown[e.event_type][reason] = (breakdown[e.event_type][reason] || 0) + 1;
    });

    res.json(breakdown);
});

export default router;
