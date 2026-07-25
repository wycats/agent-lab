import { expect, test } from '@playwright/test';
import {
  agentTurnActivityDetail,
  type AgentTurnActivityPresentation
} from '../src/lib/runs';

test('catalog activity derives a semantic browser summary from typed evidence', () => {
  const activity = {
    kind: 'capability-call',
    title: 'catalog · list',
    detail: null,
    status: 'completed',
    source: 'catalog',
    path: null,
    operation: 'list',
    callId: 'call-1',
    arguments: {},
    result: {
      items: [
        { name: 'alpha', score: 3 },
        { name: 'beta', score: 5 },
        { name: 'gamma', score: 8 }
      ]
    },
    sourceEventSequences: [3, 4]
  } satisfies AgentTurnActivityPresentation;

  expect(agentTurnActivityDetail(activity)).toBe('Returned 3 items');
});
