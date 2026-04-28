import type { Env } from '../env';
import {
  assemblePublicHomepagePayloadFromFragments,
  assemblePublicStatusPayloadFromFragments,
  readHomepageSnapshotFragments,
  readStatusSnapshotFragments,
} from '../snapshots/public-monitor-fragments';

export type ShardedPublicSnapshotKind = 'homepage' | 'status';

export type ShardedPublicSnapshotAssembleOptions = {
  env: Env;
  kind: ShardedPublicSnapshotKind;
  measureBodyBytes?: boolean;
};

export type ShardedPublicSnapshotAssembleResult = {
  ok: boolean;
  assembled: boolean;
  kind: ShardedPublicSnapshotKind;
  generatedAt?: number;
  monitorCount: number;
  invalidCount: number;
  staleCount: number;
  bodyBytes?: number | undefined;
  skip?: 'missing_envelope' | 'missing_monitors' | 'invalid_payload';
  error?: boolean;
};

function measuredBodyBytes(value: unknown, enabled: boolean): number | undefined {
  if (!enabled) {
    return undefined;
  }
  return JSON.stringify(value).length;
}

export async function assembleShardedPublicSnapshot(
  opts: ShardedPublicSnapshotAssembleOptions,
): Promise<ShardedPublicSnapshotAssembleResult> {
  try {
    if (opts.kind === 'homepage') {
      const fragments = await readHomepageSnapshotFragments(opts.env.DB);
      if (!fragments.envelope) {
        return {
          ok: true,
          assembled: false,
          kind: opts.kind,
          monitorCount: 0,
          invalidCount: fragments.monitors.invalidCount,
          staleCount: fragments.monitors.staleCount,
          skip: 'missing_envelope',
        };
      }

      const assembled = assemblePublicHomepagePayloadFromFragments(
        fragments.envelope.data,
        fragments.monitors.data,
      );
      if (!assembled) {
        return {
          ok: true,
          assembled: false,
          kind: opts.kind,
          generatedAt: fragments.envelope.generatedAt,
          monitorCount: fragments.monitors.data.length,
          invalidCount: fragments.monitors.invalidCount,
          staleCount: fragments.monitors.staleCount,
          skip:
            fragments.monitors.data.length < fragments.envelope.data.monitor_ids.length
              ? 'missing_monitors'
              : 'invalid_payload',
        };
      }

      return {
        ok: true,
        assembled: true,
        kind: opts.kind,
        generatedAt: assembled.generated_at,
        monitorCount: assembled.monitors.length,
        invalidCount: fragments.monitors.invalidCount,
        staleCount: fragments.monitors.staleCount,
        ...(opts.measureBodyBytes
          ? { bodyBytes: measuredBodyBytes(assembled, true) }
          : {}),
      };
    }

    const fragments = await readStatusSnapshotFragments(opts.env.DB);
    if (!fragments.envelope) {
      return {
        ok: true,
        assembled: false,
        kind: opts.kind,
        monitorCount: 0,
        invalidCount: fragments.monitors.invalidCount,
        staleCount: fragments.monitors.staleCount,
        skip: 'missing_envelope',
      };
    }

    const assembled = assemblePublicStatusPayloadFromFragments(
      fragments.envelope.data,
      fragments.monitors.data,
    );
    if (!assembled) {
      return {
        ok: true,
        assembled: false,
        kind: opts.kind,
        generatedAt: fragments.envelope.generatedAt,
        monitorCount: fragments.monitors.data.length,
        invalidCount: fragments.monitors.invalidCount,
        staleCount: fragments.monitors.staleCount,
        skip:
          fragments.monitors.data.length < fragments.envelope.data.monitor_ids.length
            ? 'missing_monitors'
            : 'invalid_payload',
      };
    }

    return {
      ok: true,
      assembled: true,
      kind: opts.kind,
      generatedAt: assembled.generated_at,
      monitorCount: assembled.monitors.length,
      invalidCount: fragments.monitors.invalidCount,
      staleCount: fragments.monitors.staleCount,
      ...(opts.measureBodyBytes ? { bodyBytes: measuredBodyBytes(assembled, true) } : {}),
    };
  } catch (err) {
    console.warn('internal sharded public snapshot assembly failed', err);
    return {
      ok: false,
      assembled: false,
      kind: opts.kind,
      monitorCount: 0,
      invalidCount: 0,
      staleCount: 0,
      error: true,
    };
  }
}
