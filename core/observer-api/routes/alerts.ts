import fs from 'fs';
import { Router, Request, Response } from 'express';
import { readJSONL } from '../readers/JSONLReader.js';
import { DATA_PATHS } from '../config.js';
import path from 'path';
import { alertManager, sendAlert, AlertSeverity, AlertType } from '../../alerts/index.js';

const router = Router();

// 7) GET /alerts?last=24h
router.get('/', async (req, res) => {
    const lastParam = req.query.last as string || '24h';
    let cutoffMs = 24 * 60 * 60 * 1000;

    if (lastParam.endsWith('h')) {
        cutoffMs = parseInt(lastParam.replace('h', '')) * 60 * 60 * 1000;
    } else if (lastParam.endsWith('m')) {
        cutoffMs = parseInt(lastParam.replace('m', '')) * 1000 * 60;
    }

    const now = Date.now();
    const startTime = now - cutoffMs;

    const alertsLog = path.join(DATA_PATHS.logs, 'alerts.jsonl');
    const sentLog = path.join(DATA_PATHS.outbox, 'sent.jsonl');

    // 1. Read Alerts
    const alerts = await readJSONL(alertsLog, {
        filter: (a) => new Date(a.timestamp).getTime() >= startTime
    });

    const sentMessages = await readJSONL(sentLog);
    const sentMap = new Map();
    sentMessages.forEach(m => {
        if (!sentMap.has(m.id)) sentMap.set(m.id, []);
        sentMap.get(m.id).push(m.delivered_at);
    });

    const relevantDates = Array.from(new Set(alerts.map(a => a.date)));
    const alertToMsg = new Map<string, string>();

    for (const date of relevantDates) {
        const msgPath = path.join(DATA_PATHS.messages, `message_${date}.json`);
        try {
            const msgContent = JSON.parse(fs.readFileSync(msgPath, 'utf8'));
            if (msgContent.alerts && msgContent.alerts.alert_ids) {
                msgContent.alerts.alert_ids.forEach((aid: string) => {
                    alertToMsg.set(aid, date);
                });
            }
        } catch (e) { }
    }

    const results = alerts.map(a => {
        const msgDate = alertToMsg.get(a.alert_id);
        const delivery = sentMessages.find(m => m.date === msgDate);

        return {
            alert_id: a.alert_id,
            type: a.type,
            timestamp: a.timestamp,
            delivered: !!delivery,
            delivery_time: delivery ? delivery.delivered_at : null
        };
    });

    res.json(results);
});

// GET /alerts/status - Get alert system configuration status
router.get('/status', (req: Request, res: Response) => {
    const status = alertManager.getStatus();
    res.json({
        ...status,
        alert_types: Object.keys(AlertType),
        severity_levels: Object.keys(AlertSeverity)
    });
});

// POST /alerts/test - Send a test alert (useful for verifying Slack webhook)
router.post('/test', async (req: Request, res: Response) => {
    const { severity = 'info', message } = req.body || {};

    const validSeverities = ['info', 'warning', 'error', 'critical'];
    if (!validSeverities.includes(severity)) {
        return res.status(400).json({
            error: 'INVALID_SEVERITY',
            valid: validSeverities
        });
    }

    try {
        const result = await sendAlert({
            type: 'TEST_ALERT' as any,
            severity: severity as any,
            message: message || `Test alert at ${new Date().toISOString()}`,
            source: 'observer-api',
            metadata: {
                test: true,
                triggered_by: req.ip
            }
        });

        res.json({
            status: 'sent',
            ...result
        });
    } catch (err: any) {
        res.status(500).json({
            error: 'ALERT_SEND_FAILED',
            message: err.message
        });
    }
});

export default router;
