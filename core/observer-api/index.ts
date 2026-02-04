import express, { Request, Response } from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { requestLogger } from './middleware/logger.js';
import { authGuard } from './middleware/auth.js';

// ENV must be loaded before routes
import './config.js';

// Route Imports
import healthRoutes from './routes/health.js';
import gateRoutes from './routes/gates.js';
import decisionRoutes from './routes/decisions.js';
import alertRoutes from './routes/alerts.js';
import jobsRoutes from './routes/jobs.js';
import experimentsRoutes from './routes/experiments.js';
import candidatesRoutes from './routes/candidates.js';
import apiRoutes from './routes/api.js';
import runsRoutes from './routes/runs.js';
import mlRoutes from './routes/ml.js';

// ENV GUARD
if (process.env.OBSERVER_MODE !== '1') {
    console.error('CRITICAL: OBSERVER_MODE=1 is required to start the API.');
    process.exit(1);
}

const app = express();
const port = process.env.OBSERVER_PORT || 3000;
const host = '0.0.0.0';

app.use(cors({
    origin: '*',
    methods: ['GET', 'POST'],
}));

app.use(requestLogger);
app.use(express.json());

// Routes
app.use('/health', healthRoutes);
app.use('/pipeline', healthRoutes);
app.use('/gates', gateRoutes);
app.use('/decisions', decisionRoutes);
app.use('/alerts', alertRoutes);
app.use('/debug', healthRoutes); // debug endpoint is in health.ts
app.use('/', runsRoutes); // run monitoring (migrated from core/observer)

// v1 API (secured)
app.use('/v1', authGuard);
app.use('/v1', jobsRoutes);
app.use('/v1', experimentsRoutes);
app.use('/v1', candidatesRoutes);
app.use('/v1', mlRoutes);

app.use('/api', authGuard);
app.use('/api', apiRoutes);

app.get('/ping', (req: Request, res: Response) => {
    res.json({ status: 'pong', mode: 'READ-ONLY', time: new Date().toISOString() });
});

const server = createServer(app);

const shutdown = () => {
    console.log('Shutting down Observer API...');
    server.close(() => {
        console.log('Server closed.');
        process.exit(0);
    });

    setTimeout(() => {
        process.exit(1);
    }, 5000);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

server.listen(Number(port), host, () => {
    console.log(`============================================================`);
    console.log(`QuantLab Observer API`);
    console.log(`Mode: READ-ONLY (Verified)`);
    console.log(`Listening on http://${host}:${port}`);
    console.log(`============================================================`);
});
