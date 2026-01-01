/**
 * telegram.js: Telegram delivery adapter for QualtLab Ops.
 */

/**
 * Delivers an operational message via Telegram Bot API.
 * @param {Object} envelope - The message envelope from outbox.jsonl
 * @param {Object} opts - Optional configuration overrides
 * @returns {Promise<Object>} { success: boolean, body?: string, reason?: string }
 */
export async function deliver(envelope, opts = {}) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  const disablePreview = process.env.TELEGRAM_DISABLE_PREVIEW !== 'false'; // Default to true

  if (!token || !chatId) {
    return { success: false, reason: 'MISSING_TELEGRAM_ENV' };
  }

  const payload = envelope.payload || {};
  const status = payload.status || {};
  const alerts = payload.alerts || { count: 0 };
  const retention = payload.retention || {};
  const date = envelope.date || 'unknown';

  // 1. Build Formatted Plain Text
  const header = `QUANTLAB OPS — ${date}`;
  const statusLine = `${status.health_status || 'UNKNOWN'} | Run:${status.run_status || '?'} | Promo:${status.promotion_result || '?'} | Alerts:${alerts.count}`;
  
  const body = (payload.summary_lines || []).join('\n');
  
  let footer = '';
  if (retention.plan_found) {
    footer = `\nRetention: ${retention.candidates_total} candidates`;
  } else {
    footer = `\nRetention: no recent plan`;
  }

  const fullText = `${header}\n${statusLine}\n\n${body}\n${footer}`;

  // 2. Send via Fetch
  const url = `https://api.telegram.org/bot${token}/sendMessage`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 10000); // 10s timeout

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: fullText,
        disable_web_page_preview: disablePreview
      }),
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    const data = await response.json();

    if (response.ok && data.ok) {
      return { success: true, body: fullText };
    } else {
      const reason = data.description ? `TELEGRAM_API_${data.description.replace(/\s+/g, '_').toUpperCase()}` : `TELEGRAM_HTTP_${response.status}`;
      return { success: false, reason: reason.substring(0, 50) };
    }
  } catch (e) {
    clearTimeout(timeoutId);
    if (e.name === 'AbortError') {
      return { success: false, reason: 'TELEGRAM_TIMEOUT' };
    }
    return { success: false, reason: 'TELEGRAM_FETCH_ERROR' };
  }
}
