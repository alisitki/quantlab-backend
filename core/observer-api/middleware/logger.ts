import { Request, Response, NextFunction } from 'express';
import { randomUUID } from 'crypto';

export const requestLogger = (req: Request, res: Response, next: NextFunction) => {
    const requestId = randomUUID();
    const start = Date.now();

    // Attach ID to response
    res.setHeader('x-request-id', requestId);

    // Log on finish
    res.on('finish', () => {
        const duration = Date.now() - start;
        const dateParam = req.query.date || 'N/A';
        console.log(`[REQ] ${requestId} | ${req.method} ${req.path} | date=${dateParam} | status=${res.statusCode} | ${duration}ms`);
    });

    next();
};
