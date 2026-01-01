import fs from 'fs';
import path from 'path';

export function readDailyJson(directory: string, date: string, prefix: string): any | null {
    const fileName = `${prefix}${date}.json`;
    const filePath = path.join(directory, fileName);

    if (!fs.existsSync(filePath)) {
        return null;
    }

    try {
        return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (e) {
        return null;
    }
}

export function findLatestDailyJson(directory: string, prefix: string): any | null {
    if (!fs.existsSync(directory)) return null;

    const files = fs.readdirSync(directory)
        .filter(f => f.startsWith(prefix) && f.endsWith('.json'))
        .sort()
        .reverse();

    if (files.length === 0) return null;

    return readDailyJson(directory, files[0].replace(prefix, '').replace('.json', ''), prefix);
}
