/**
 * QuantLab Strategy Runtime — State Serializer
 * 
 * PHASE 1: Determinism Foundation
 * 
 * Provides canonical JSON serialization for deterministic hashing.
 * Critical invariant: stringify(parse(x)) === x
 * 
 * Features:
 * - Sorted object keys for deterministic output
 * - BigInt → string conversion (preserves precision)
 * - String → BigInt restoration
 * - No prototype pollution
 * 
 * @module core/strategy/state/StateSerializer
 */

/**
 * Canonical JSON stringify with sorted keys and BigInt support.
 * 
 * Rules:
 * - Object keys are sorted alphabetically
 * - BigInt values are converted to strings with 'n' suffix
 * - Undefined values are omitted
 * - Null is preserved
 * - Arrays maintain order
 * 
 * @param {any} value - Value to stringify
 * @returns {string} Canonical JSON string
 */
export function canonicalStringify(value) {
  return JSON.stringify(value, replacer, 0);
}

/**
 * Pretty-print canonical JSON (for debugging)
 * 
 * @param {any} value - Value to stringify
 * @param {number} [indent=2] - Indentation spaces
 * @returns {string} Formatted canonical JSON string
 */
export function canonicalStringifyPretty(value, indent = 2) {
  return JSON.stringify(value, replacer, indent);
}

/**
 * Parse JSON with BigInt restoration.
 * 
 * Converts strings ending with 'n' back to BigInt.
 * 
 * @param {string} json - JSON string to parse
 * @returns {any} Parsed value with BigInt restored
 */
export function canonicalParse(json) {
  return JSON.parse(json, reviver);
}

/**
 * JSON replacer function for canonical serialization.
 * Handles BigInt and sorts object keys.
 * 
 * @param {string} key - Object key
 * @param {any} value - Value to serialize
 * @returns {any} Serialized value
 */
function replacer(key, value) {
  // Handle BigInt
  if (typeof value === 'bigint') {
    return value.toString() + 'n';
  }
  
  // Handle objects (sort keys)
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    const sorted = {};
    const keys = Object.keys(value).sort();
    for (const k of keys) {
      // Skip undefined values
      if (value[k] !== undefined) {
        sorted[k] = value[k];
      }
    }
    return sorted;
  }
  
  return value;
}

/**
 * JSON reviver function for BigInt restoration.
 * 
 * @param {string} key - Object key
 * @param {any} value - Parsed value
 * @returns {any} Restored value
 */
function reviver(key, value) {
  // Restore BigInt from strings ending with 'n'
  if (typeof value === 'string' && /^-?\d+n$/.test(value)) {
    return BigInt(value.slice(0, -1));
  }
  return value;
}

/**
 * Check if two values are canonically equal.
 * Uses canonical stringify for comparison.
 * 
 * @param {any} a - First value
 * @param {any} b - Second value
 * @returns {boolean} True if canonically equal
 */
export function canonicalEquals(a, b) {
  return canonicalStringify(a) === canonicalStringify(b);
}

/**
 * Deep clone using canonical serialization.
 * Ensures deterministic cloning with BigInt support.
 * 
 * @param {any} value - Value to clone
 * @returns {any} Cloned value
 */
export function canonicalClone(value) {
  return canonicalParse(canonicalStringify(value));
}

/**
 * Create a frozen (immutable) deep clone.
 * 
 * @param {any} value - Value to clone and freeze
 * @returns {any} Frozen cloned value
 */
export function immutableSnapshot(value) {
  const cloned = canonicalClone(value);
  return deepFreeze(cloned);
}

/**
 * Deep freeze an object and all nested objects.
 * 
 * @param {any} obj - Object to freeze
 * @returns {any} Frozen object
 */
function deepFreeze(obj) {
  if (obj === null || typeof obj !== 'object') {
    return obj;
  }
  
  Object.freeze(obj);
  
  if (Array.isArray(obj)) {
    for (const item of obj) {
      deepFreeze(item);
    }
  } else {
    for (const key of Object.keys(obj)) {
      deepFreeze(obj[key]);
    }
  }
  
  return obj;
}

// Default export for convenience
export default {
  stringify: canonicalStringify,
  stringifyPretty: canonicalStringifyPretty,
  parse: canonicalParse,
  equals: canonicalEquals,
  clone: canonicalClone,
  immutableSnapshot
};
