/**
 * @entangled/react — pendingOps.ts
 *
 * Tracks optimistic mutations independently from React Query cache.
 * This ensures pendingOps survive React Query invalidations (from delta pushes).
 *
 * Key design:
 * - pendingOps live in a ref, NOT in React Query cache
 * - Each op carries a requestId for server correlation
 * - When delta push arrives with matching requestId → op confirmed
 * - Fallback: mutation onSuccess also clears the op
 * - Safety net: 30s timeout auto-clears stale ops
 */

import type { Entangled } from '@entangled/protocol';

// ── Internal pending operation ──────────────────────────────────

export interface PendingOp<T = any> {
  id: string;                  // tempId for create, realId for update/delete
  requestId: string;           // WS request_id — for delta correlation
  op: 'create' | 'update' | 'delete';
  data: Partial<T>;            // optimistic data
  status: 'pending' | 'failed';
  error?: string;
  retryFn?: () => void;
  startedAt: number;
}

// ── Merge: confirmed + pending → Entangled<T>[] ─────────────────

export function mergeWithPending<T>(
  confirmed: T[],
  pending: PendingOp<T>[],
  getId: (item: T) => string,
): Entangled<T>[] {
  // Start with confirmed items, all marked as confirmed
  const result: Entangled<T>[] = confirmed.map(item => ({
    ...item,
    _status: 'confirmed' as const,
  }));

  for (const op of pending) {
    switch (op.op) {
      case 'create': {
        // If server already confirmed (item in confirmed list via delta),
        // skip this pending op — will be cleaned up by onSuccess or confirmByRequestId
        const existsInConfirmed = result.some(i => getId(i) === op.id);
        if (existsInConfirmed) continue;

        result.push({
          ...op.data as T,
          _status: op.status,
          _op: 'create',
          _tempId: op.id,
          _error: op.error,
          _retry: op.retryFn,
        });
        break;
      }

      case 'update': {
        const idx = result.findIndex(i => getId(i) === op.id);
        if (idx >= 0) {
          // Field-level merge: confirmed base + pending changes
          result[idx] = {
            ...result[idx],      // server's latest for all fields
            ...op.data,          // only override the fields we're modifying
            _status: op.status,
            _op: 'update',
            _error: op.error,
            _retry: op.retryFn,
          };
        }
        break;
      }

      case 'delete': {
        const idx = result.findIndex(i => getId(i) === op.id);
        if (idx >= 0) {
          result[idx] = {
            ...result[idx],
            _status: op.status,
            _op: 'delete',
            _error: op.error,
            _retry: op.retryFn,
          };
        }
        // If item not in confirmed (already deleted by delta), skip
        break;
      }
    }
  }

  return result;
}

// ── Confirm pending ops by requestId (from delta push) ──────────

export function confirmByRequestIds<T>(
  ops: PendingOp<T>[],
  requestIds: string[],
): PendingOp<T>[] {
  if (!requestIds.length) return ops;
  const idSet = new Set(requestIds);
  return ops.filter(op => !idSet.has(op.requestId));
}

// ── Timeout cleanup (30s safety net) ────────────────────────────

export function cleanupStaleOps<T>(
  ops: PendingOp<T>[],
  maxAgeMs: number = 30_000,
): PendingOp<T>[] {
  const now = Date.now();
  return ops.filter(op =>
    op.status === 'failed' || (now - op.startedAt) < maxAgeMs,
  );
}

// ── Generate unique request ID ──────────────────────────────────

let _counter = 0;
export function genRequestId(): string {
  return `rq_${Date.now()}_${++_counter}`;
}
