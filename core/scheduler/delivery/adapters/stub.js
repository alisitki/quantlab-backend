/**
 * stub.js: Simulated delivery adapter v1.
 */

/**
 * Simulates delivery of an operational message envelope.
 * @param {Object} envelope - The message envelope from outbox.jsonl
 * @returns {Object} { success: boolean, reason?: string, body: string }
 */
export function deliver(envelope) {
  const payload = envelope.payload || {};
  const body = (payload.summary_lines || []).join(' | ');
  
  // Simulated failure condition based on health_status
  if (payload.status?.health_status === 'FAILED_SAFETY') {
    return { 
      success: false, 
      reason: 'SIMULATED_FAILED_SAFETY', 
      body 
    };
  }
  
  return { 
    success: true, 
    body 
  };
}
