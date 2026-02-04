/**
 * Release Config Sanity Check
 */

function envBool(val) {
  return val === '1' || val === 'true' || val === 'yes';
}

function requireString(value, name, errors) {
  if (!value || typeof value !== 'string') {
    errors.push(`${name} is required`);
  }
}

export function runConfigCheck() {
  const errors = [];

  requireString(process.env.OBSERVER_TOKEN, 'OBSERVER_TOKEN', errors);

  const archiveEnabled = envBool(process.env.RUN_ARCHIVE_ENABLED || '0');
  if (archiveEnabled) {
    requireString(process.env.RUN_ARCHIVE_S3_BUCKET, 'RUN_ARCHIVE_S3_BUCKET', errors);
    requireString(process.env.RUN_ARCHIVE_S3_ENDPOINT, 'RUN_ARCHIVE_S3_ENDPOINT', errors);
    requireString(process.env.RUN_ARCHIVE_S3_ACCESS_KEY, 'RUN_ARCHIVE_S3_ACCESS_KEY', errors);
    requireString(process.env.RUN_ARCHIVE_S3_SECRET_KEY, 'RUN_ARCHIVE_S3_SECRET_KEY', errors);
  }

  if (errors.length > 0) {
    for (const msg of errors) {
      console.error(`[config] ${msg}`);
    }
    process.exit(1);
  }

  console.log(JSON.stringify({
    event: 'config_check',
    status: 'ok'
  }));
}

if (process.argv[1] && process.argv[1].endsWith('ConfigCheck.js')) {
  runConfigCheck();
}
