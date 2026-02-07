/**
 * Tests for LifecycleStage module
 *
 * Verifies stage enum, transition validation, and utility functions.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  LifecycleStage,
  STAGE_ORDER,
  VALID_PROMOTIONS,
  VALID_DEMOTIONS,
  getNextStage,
  getPrevStage,
  canPromote,
  canDemote,
  isTerminal,
  getStageIndex,
  isValidStage,
  getAllStages
} from '../LifecycleStage.js';

describe('LifecycleStage', () => {
  it('should define all required stages', () => {
    assert.equal(LifecycleStage.CANDIDATE, 'CANDIDATE');
    assert.equal(LifecycleStage.PAPER, 'PAPER');
    assert.equal(LifecycleStage.CANARY, 'CANARY');
    assert.equal(LifecycleStage.SHADOW, 'SHADOW');
    assert.equal(LifecycleStage.LIVE, 'LIVE');
    assert.equal(LifecycleStage.RETIRED, 'RETIRED');
  });

  it('should have correct stage order', () => {
    assert.deepEqual(STAGE_ORDER, ['CANDIDATE', 'PAPER', 'CANARY', 'SHADOW', 'LIVE']);
  });

  it('should validate promotions correctly', () => {
    // Valid promotions
    assert.ok(canPromote('CANDIDATE', 'PAPER'));
    assert.ok(canPromote('PAPER', 'CANARY'));
    assert.ok(canPromote('CANARY', 'SHADOW'));
    assert.ok(canPromote('SHADOW', 'LIVE'));

    // Invalid promotions
    assert.equal(canPromote('CANDIDATE', 'CANARY'), false);
    assert.equal(canPromote('LIVE', 'RETIRED'), false);
    assert.equal(canPromote('RETIRED', 'CANDIDATE'), false);
  });

  it('should validate demotions correctly', () => {
    // Valid demotions
    assert.ok(canDemote('PAPER', 'CANDIDATE'));
    assert.ok(canDemote('CANARY', 'PAPER'));
    assert.ok(canDemote('SHADOW', 'CANARY'));
    assert.ok(canDemote('LIVE', 'SHADOW'));

    // Valid retirements
    assert.ok(canDemote('CANDIDATE', 'RETIRED'));
    assert.ok(canDemote('PAPER', 'RETIRED'));
    assert.ok(canDemote('LIVE', 'RETIRED'));

    // Invalid demotions
    assert.equal(canDemote('CANDIDATE', 'PAPER'), false);
    assert.equal(canDemote('RETIRED', 'CANDIDATE'), false);
  });

  it('should get next and previous stages correctly', () => {
    // Next stages
    assert.equal(getNextStage('CANDIDATE'), 'PAPER');
    assert.equal(getNextStage('PAPER'), 'CANARY');
    assert.equal(getNextStage('SHADOW'), 'LIVE');
    assert.equal(getNextStage('LIVE'), null);
    assert.equal(getNextStage('RETIRED'), null);

    // Previous stages
    assert.equal(getPrevStage('CANDIDATE'), null);
    assert.equal(getPrevStage('PAPER'), 'CANDIDATE');
    assert.equal(getPrevStage('CANARY'), 'PAPER');
    assert.equal(getPrevStage('LIVE'), 'SHADOW');
    assert.equal(getPrevStage('RETIRED'), null);
  });

  it('should identify terminal stages', () => {
    assert.equal(isTerminal('CANDIDATE'), false);
    assert.equal(isTerminal('PAPER'), false);
    assert.equal(isTerminal('CANARY'), false);
    assert.equal(isTerminal('SHADOW'), false);
    assert.equal(isTerminal('LIVE'), true);
    assert.equal(isTerminal('RETIRED'), true);
  });

  it('should return correct stage indices', () => {
    assert.equal(getStageIndex('CANDIDATE'), 0);
    assert.equal(getStageIndex('PAPER'), 1);
    assert.equal(getStageIndex('CANARY'), 2);
    assert.equal(getStageIndex('SHADOW'), 3);
    assert.equal(getStageIndex('LIVE'), 4);
    assert.equal(getStageIndex('RETIRED'), -1);
  });

  it('should validate stage strings', () => {
    // Valid stages
    assert.ok(isValidStage('CANDIDATE'));
    assert.ok(isValidStage('LIVE'));
    assert.ok(isValidStage('RETIRED'));

    // Invalid stages
    assert.equal(isValidStage('INVALID'), false);
    assert.equal(isValidStage('candidate'), false);
    assert.equal(isValidStage(''), false);
    assert.equal(isValidStage(null), false);
  });
});
