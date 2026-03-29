/**
 * @entangled/react — pendingOps.ts
 *
 * Re-exports from the unified useOptimistic engine for backward compatibility.
 *
 * The actual implementation now lives in useOptimistic.ts. This file exists
 * to avoid breaking external imports of PendingOp, mergeWithPending, etc.
 */

export {
  type PendingOp,
  mergeWithPending,
  mergeStreamPending,
  genRequestId,
  useOptimisticOps,
  type OptimisticOps,
  type UseOptimisticConfig,
} from './useOptimistic';

/**
 * Confirm pending ops by requestId (from delta push).
 * @deprecated Use `useOptimisticOps` with `confirmMode: 'requestId'` instead.
 * Kept for backward compatibility with direct consumers.
 */
export function confirmByRequestIds<T>(
  ops: Array<{ requestId: string } & Record<string, any>>,
  requestIds: string[],
): any[] {
  if (!requestIds.length) return ops;
  const idSet = new Set(requestIds);
  return ops.filter(op => !idSet.has(op.requestId));
}

/**
 * Cleanup stale pending ops.
 * @deprecated Handled automatically by `useOptimisticOps` timeout cleanup.
 */
export function cleanupStaleOps<T>(
  ops: Array<{ status: string; startedAt: number } & Record<string, any>>,
  maxAgeMs: number = 30_000,
  failedMaxAgeMs: number = 5 * 60_000,
): any[] {
  const now = Date.now();
  return ops.filter(op => {
    const age = now - op.startedAt;
    if (op.status === 'failed') return age < failedMaxAgeMs;
    return age < maxAgeMs;
  });
}
