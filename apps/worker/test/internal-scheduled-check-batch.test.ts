import { afterEach, describe, expect, it, vi } from 'vitest';

vi.mock('../src/scheduler/scheduled', () => ({
  runExclusivePersistedMonitorBatch: vi.fn(),
}));

import type { Env } from '../src/env';
import worker from '../src/index';
import { runExclusivePersistedMonitorBatch } from '../src/scheduler/scheduled';
import { createFakeD1Database } from './helpers/fake-d1';

describe('internal scheduled check-batch route', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.clearAllMocks();
  });

  it('rejects stale and future checked_at values even with a valid token', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(new Date('2026-04-15T05:18:20.000Z').valueOf());

    const env = {
      DB: createFakeD1Database([]),
      ADMIN_TOKEN: 'test-admin-token',
    } as unknown as Env;

    const makeRequest = (checkedAt: number) =>
      worker.fetch(
        new Request('http://internal/api/v1/internal/scheduled/check-batch', {
          method: 'POST',
          headers: {
            Authorization: 'Bearer test-admin-token',
            'Content-Type': 'application/json; charset=utf-8',
          },
          body: JSON.stringify({
            token: 'test-admin-token',
            ids: [1],
            checked_at: checkedAt,
            state_failures_to_down_from_up: 2,
            state_successes_to_up_from_down: 2,
          }),
        }),
        env,
        { waitUntil: vi.fn() } as unknown as ExecutionContext,
      );

    await expect(makeRequest(1_776_230_340)).resolves.toMatchObject({ status: 403 });
    await expect(makeRequest(1_776_230_160)).resolves.toMatchObject({ status: 403 });
  });

  it('returns compact runtime updates when requested by the scheduler service', async () => {
    const now = new Date('2026-04-15T05:18:20.000Z').valueOf();
    vi.spyOn(Date, 'now').mockReturnValue(now);

    vi.mocked(runExclusivePersistedMonitorBatch).mockResolvedValue({
      runtimeUpdates: [
        {
          monitor_id: 1,
          interval_sec: 60,
          created_at: 1_776_230_000,
          checked_at: 1_776_230_280,
          check_status: 'up',
          next_status: 'up',
          latency_ms: 21,
        },
      ],
      stats: {
        processedCount: 1,
        rejectedCount: 0,
        attemptTotal: 1,
        httpCount: 1,
        tcpCount: 0,
        assertionCount: 0,
        downCount: 0,
        unknownCount: 0,
      },
      checksDurMs: 4,
      persistDurMs: 2,
    });

    const env = {
      DB: createFakeD1Database([]),
      ADMIN_TOKEN: 'test-admin-token',
    } as unknown as Env;

    const res = await worker.fetch(
      new Request('http://internal/api/v1/internal/scheduled/check-batch', {
        method: 'POST',
        headers: {
          Authorization: 'Bearer test-admin-token',
          'Content-Type': 'application/json; charset=utf-8',
          'X-Uptimer-Internal-Format': 'compact-v1',
        },
        body: JSON.stringify({
          token: 'test-admin-token',
          ids: [1],
          checked_at: 1_776_230_280,
          state_failures_to_down_from_up: 2,
          state_successes_to_up_from_down: 2,
        }),
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toMatchObject({
      ok: true,
      runtime_updates: [[1, 60, 1_776_230_000, 1_776_230_280, 'up', 'up', 21]],
      processed_count: 1,
      checks_duration_ms: 4,
      persist_duration_ms: 2,
    });
  });
});
