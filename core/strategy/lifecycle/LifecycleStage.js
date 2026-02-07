/**
 * Strategy Lifecycle Stage Definitions and Transitions
 *
 * Defines the stage enum, valid transitions, and utility functions
 * for strategy lifecycle management.
 *
 * Stage progression:
 * CANDIDATE → PAPER → CANARY → SHADOW → LIVE → RETIRED
 */

/**
 * Lifecycle stage enum
 * @readonly
 */
export const LifecycleStage = Object.freeze({
  CANDIDATE: 'CANDIDATE',
  PAPER: 'PAPER',
  CANARY: 'CANARY',
  SHADOW: 'SHADOW',
  LIVE: 'LIVE',
  RETIRED: 'RETIRED'
});

/**
 * Ordered stages (excludes RETIRED which is terminal)
 * @type {string[]}
 */
export const STAGE_ORDER = ['CANDIDATE', 'PAPER', 'CANARY', 'SHADOW', 'LIVE'];

/**
 * Valid promotion transitions (forward movement)
 * @type {Object.<string, string[]>}
 */
export const VALID_PROMOTIONS = {
  CANDIDATE: ['PAPER'],
  PAPER: ['CANARY'],
  CANARY: ['SHADOW'],
  SHADOW: ['LIVE'],
  LIVE: [],
  RETIRED: []
};

/**
 * Valid demotion transitions (backward movement or retirement)
 * @type {Object.<string, string[]>}
 */
export const VALID_DEMOTIONS = {
  CANDIDATE: ['RETIRED'],
  PAPER: ['CANDIDATE', 'RETIRED'],
  CANARY: ['PAPER', 'RETIRED'],
  SHADOW: ['CANARY', 'RETIRED'],
  LIVE: ['SHADOW', 'RETIRED'],
  RETIRED: []
};

/**
 * Get the next stage in progression
 * @param {string} currentStage
 * @returns {string|null} Next stage or null if at LIVE
 */
export function getNextStage(currentStage) {
  const promotions = VALID_PROMOTIONS[currentStage];
  return promotions && promotions.length > 0 ? promotions[0] : null;
}

/**
 * Get the previous stage in progression
 * @param {string} currentStage
 * @returns {string|null} Previous stage or null if at CANDIDATE
 */
export function getPrevStage(currentStage) {
  const currentIndex = STAGE_ORDER.indexOf(currentStage);
  if (currentIndex <= 0) return null;
  return STAGE_ORDER[currentIndex - 1];
}

/**
 * Check if promotion is valid
 * @param {string} currentStage
 * @param {string} targetStage
 * @returns {boolean}
 */
export function canPromote(currentStage, targetStage) {
  const validTargets = VALID_PROMOTIONS[currentStage] || [];
  return validTargets.includes(targetStage);
}

/**
 * Check if demotion is valid
 * @param {string} currentStage
 * @param {string} targetStage
 * @returns {boolean}
 */
export function canDemote(currentStage, targetStage) {
  const validTargets = VALID_DEMOTIONS[currentStage] || [];
  return validTargets.includes(targetStage);
}

/**
 * Check if stage is terminal (no further transitions)
 * @param {string} stage
 * @returns {boolean}
 */
export function isTerminal(stage) {
  return stage === LifecycleStage.RETIRED || stage === LifecycleStage.LIVE;
}

/**
 * Get stage index in progression order
 * @param {string} stage
 * @returns {number} Index in STAGE_ORDER, or -1 if RETIRED
 */
export function getStageIndex(stage) {
  if (stage === LifecycleStage.RETIRED) return -1;
  return STAGE_ORDER.indexOf(stage);
}

/**
 * Check if a stage string is valid
 * @param {string} stage
 * @returns {boolean}
 */
export function isValidStage(stage) {
  return Object.values(LifecycleStage).includes(stage);
}

/**
 * Get all valid stages
 * @returns {string[]}
 */
export function getAllStages() {
  return Object.values(LifecycleStage);
}
