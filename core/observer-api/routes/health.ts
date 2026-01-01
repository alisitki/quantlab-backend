import { Router } from 'express';
import { findLatestDailyJson, readDailyJson } from '../readers/DailyFileReader.js';
import { DATA_PATHS, S3_CONFIG, buildCompactKey, buildFeaturesKey, CONFIG_METADATA } from '../config.js';
import { S3Client, HeadObjectCommand } from '@aws-sdk/client-s3';

const router = Router();

const s3 = new S3Client({
    endpoint: S3_CONFIG.endpoint,
    region: S3_CONFIG.region,
    credentials: S3_CONFIG.credentials,
    forcePathStyle: S3_CONFIG.forcePathStyle
});

async function checkS3(key: string): Promise<string> {
    try {
        await s3.send(new HeadObjectCommand({ Bucket: S3_CONFIG.bucket, Key: key }));
        console.log(`[S3] EXISTS | Key: ${key}`);
        return 'EXISTS';
    } catch (e: any) {
        if (e.name === 'NotFound') {
            console.log(`[S3] MISSING | Key: ${key}`);
            return 'MISSING';
        }
        throw e;
    }
}

function getYesterdayIstanbul(): string {
    const now = new Date();
    // Istanbul is UTC+3. To get "yesterday" in Istanbul, subtract 24 + shift
    const shifted = new Date(now.getTime() + (3 * 60 * 60 * 1000) - (24 * 60 * 60 * 1000));
    return shifted.toISOString().split('T')[0].replace(/-/g, '');
}

// 1) GET /health/today
router.get('/today', (req, res) => {
    const latestHealth = findLatestDailyJson(DATA_PATHS.health, 'daily_');

    if (!latestHealth) {
        return res.status(404).json({ error: 'No health data found' });
    }

    res.json({
        status: latestHealth.health_status,
        components: latestHealth.alerts?.types || [],
        date: latestHealth.date
    });
});

// 2) GET /pipeline/status?date=YYYYMMDD
router.get('/status', async (req, res) => {
    let date = req.query.date as string;

    // Default to yesterday Istanbul if missing
    if (!date) {
        date = getYesterdayIstanbul();
    }

    if (!/^\d{8}$/.test(date)) {
        return res.status(400).json({ error: 'Valid date parameter YYYYMMDD is required' });
    }

    if (!S3_CONFIG.endpoint || !S3_CONFIG.credentials.accessKeyId) {
        return res.status(500).json({ error: 'S3 credentials not configured' });
    }

    try {
        const compactKey = buildCompactKey(date);
        const featureKey = buildFeaturesKey(date);

        const [compactHead, featuresHead] = await Promise.all([
            checkS3(compactKey),
            checkS3(featureKey)
        ]);

        const health = readDailyJson(DATA_PATHS.health, date, 'daily_');
        const shadow = readDailyJson(DATA_PATHS.reports, date, 'shadow_');

        res.json({
            date,
            resolved_date: date,
            compaction_status: compactHead === 'EXISTS' ? 'READY' : 'NOT_READY',
            features_present: featuresHead === 'EXISTS',
            shadow_run_status: shadow ? (shadow.determinism_verified ? 'SUCCESS' : 'FAILED') : 'N/A',
            sources: {
                health_file: health ? `health/daily_${date}.json` : null,
                shadow_file: shadow ? `reports/shadow/daily/shadow_${date}.json` : null,
                compact_key: compactKey,
                features_key: featureKey,
                bucket: S3_CONFIG.bucket,
                endpoint_host: CONFIG_METADATA.endpoint,
                key_scheme_version: CONFIG_METADATA.key_scheme_version
            }
        });
    } catch (e: any) {
        console.error(`[S3 ERROR] ${e.message}`);
        res.status(500).json({ error: `S3 check failed: ${e.message}` });
    }
});

// 3) GET /debug/config
router.get('/config', (req, res) => {
    res.json(CONFIG_METADATA);
});

export default router;
