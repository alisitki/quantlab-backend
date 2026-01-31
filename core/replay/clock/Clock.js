/**
 * QuantLab Replay Engine — Clock Interface
 * 
 * Clocks control replay timing without affecting event order.
 * RULE: Clock ONLY affects timing, NEVER event sequence.
 * 
 * @interface Clock
 */

/**
 * Initialize clock with first event timestamp.
 * Called once before replay starts.
 * 
 * @function init
 * @param {bigint|number} firstTs - First event ts_event
 * @returns {void}
 */

/**
 * Wait before emitting next event.
 * Called for each event during replay.
 * 
 * @function wait
 * @param {bigint|number} ts_event - Current event timestamp
 * @returns {Promise<void>}
 */

/**
 * Called when replay ends.
 * 
 * @function onEnd
 * @returns {void}
 */

export default {};
