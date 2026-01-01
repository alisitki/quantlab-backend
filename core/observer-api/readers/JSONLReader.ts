import fs from 'fs';
import readline from 'readline';

export interface JSONLOptions {
    filter?: (line: any) => boolean;
    limit?: number;
}

export async function readJSONL<T = any>(filePath: string, options: JSONLOptions = {}): Promise<T[]> {
    if (!fs.existsSync(filePath)) {
        return [];
    }

    const results: T[] = [];
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    for await (const line of rl) {
        if (!line.trim()) continue;
        try {
            const parsed = JSON.parse(line);
            if (!options.filter || options.filter(parsed)) {
                results.push(parsed);
            }
            if (options.limit && results.length >= options.limit) {
                break;
            }
        } catch (e) {
            // Skip invalid lines
        }
    }

    return results;
}
