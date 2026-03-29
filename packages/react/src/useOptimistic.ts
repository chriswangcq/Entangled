/**
 * @entangled/react — useOptimistic.ts
 *
 * Unified optimistic state engine shared by useList and useStream.
 *
 * Core abstractions:
 *   - PendingOp<T>: a single optimistic mutation (create/update/delete)
 *   - useOptimisticOps(): hook that manages the pending ops lifecycle
 *
 * Lifecycle of a pending op:
 *   1. Mutation starts → add PendingOp to ref → forceRender
 *   2. entities_changed arrives with matching requestId → confirm (remove) op
 *   3. Mutation succeeds → remove op (fallback if requestId confirm already did)
 *   4. Mutation fails → mark op as 'failed' with error message
 *   5. Timeout (30s pending, 5min failed) → auto-cleanup
 *
 * Both useList and useStream delegate to this engine instead of
 * maintaining their own independent optimistic state.
 */

import { useEffect, useRef, useState, useCallback } from 'react';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';

// ── PendingOp ───────────────────────────────────────────────────

export interface PendingOp<T = any> {
  id: string;                  // tempId for create, realId for update/delete
  requestId: string;           // WS request_id — for delta correlation
  op: 'create' | 'update' | 'delete';
  data: Partial<T>;            // optimistic data snapshot
  status: 'pending' | 'failed';
  error?: string;
  retryFn?: () => void;
  startedAt: number;
}

// ── Hook return type ────────────────────────────────────────────

export interface OptimisticOps<T> {
  /** Current pending ops (read-only snapshot). */
  readonly ops: PendingOp<T>[];

  /** Add a new pending op. Returns the op (for caller to track tempId). */
  addOp: (op: Omit<PendingOp<T>, 'startedAt'>) => PendingOp<T>;

  /** Remove a pending op by id + op type (on mutation success). */
  removeOp: (id: string, opType?: PendingOp['op']) => void;

  /** Remove a pending op by id only (any op type). */
  removeById: (id: string) => void;

  /** Mark a pending op as failed. */
  failOp: (id: string, opType: PendingOp['op'], error: string) => void;

  /** Force a re-render (e.g. when server data changes and merge needs update). */
  forceRender: () => void;

  /** Current renderTick for useMemo dependency. */
  renderTick: number;
}

// ── Config ──────────────────────────────────────────────────────

export interface UseOptimisticConfig {
  /** Entity name — used to filter entities_changed events. */
  entityName: string;
  /** Whether the hook is enabled. */
  enabled?: boolean;
  /** How to confirm ops: 'requestId' (useList) or 'serverIdDedup' (useStream). */
  confirmMode: 'requestId' | 'serverIdDedup';
  /** Max age for pending ops before auto-cleanup (ms). Default: 30000. */
  pendingMaxAgeMs?: number;
  /** Max age for failed ops before auto-cleanup (ms). Default: 300000 (5min). */
  failedMaxAgeMs?: number;
}

// ── Hook Implementation ─────────────────────────────────────────

export function useOptimisticOps<T>(config: UseOptimisticConfig): OptimisticOps<T> {
  const {
    entityName,
    enabled = true,
    confirmMode,
    pendingMaxAgeMs = 30_000,
    failedMaxAgeMs = 5 * 60_000,
  } = config;

  const opsRef = useRef<PendingOp<T>[]>([]);
  const [renderTick, setRenderTick] = useState(0);
  const forceRender = useCallback(() => setRenderTick((n: number) => n + 1), []);

  // ── entities_changed listener ─────────────────────────────────
  // For requestId mode: confirm ops by matching requestIds from delta push.
  // For serverIdDedup mode: force re-render so the caller's merge can deduplicate.
  useEffect(() => {
    if (!enabled) return;

    let unlisten: UnlistenFn | null = null;

    (async () => {
      unlisten = await listen<{ changes: Array<{ entity: string; requestIds?: string[] }> }>(
        'entities_changed',
        (event) => {
          for (const change of event.payload.changes) {
            if (change.entity !== entityName) continue;

            if (confirmMode === 'requestId' && change.requestIds?.length) {
              // useList mode: confirm by matching requestId
              const idSet = new Set(change.requestIds);
              const before = opsRef.current.length;
              opsRef.current = opsRef.current.filter(
                (op) => !idSet.has(op.requestId),
              );
              if (opsRef.current.length !== before) {
                forceRender();
              }
            } else if (confirmMode === 'serverIdDedup') {
              // useStream mode: force re-render so caller's merge logic can
              // compare pending IDs against server data
              if (opsRef.current.length > 0) {
                forceRender();
              }
            }
          }
        },
      );
    })();

    return () => { unlisten?.(); };
  }, [entityName, enabled, confirmMode]);

  // ── Timeout cleanup ───────────────────────────────────────────
  useEffect(() => {
    const timer = setInterval(() => {
      const now = Date.now();
      const before = opsRef.current.length;
      opsRef.current = opsRef.current.filter((op) => {
        const age = now - op.startedAt;
        if (op.status === 'failed') return age < failedMaxAgeMs;
        return age < pendingMaxAgeMs;
      });
      if (opsRef.current.length !== before) forceRender();
    }, 5000);
    return () => clearInterval(timer);
  }, [pendingMaxAgeMs, failedMaxAgeMs]);

  // ── Mutation helpers ──────────────────────────────────────────

  const addOp = useCallback((op: Omit<PendingOp<T>, 'startedAt'>): PendingOp<T> => {
    const fullOp: PendingOp<T> = { ...op, startedAt: Date.now() };
    opsRef.current = [...opsRef.current, fullOp];
    forceRender();
    return fullOp;
  }, []);

  const removeOp = useCallback((id: string, opType?: PendingOp['op']) => {
    const before = opsRef.current.length;
    opsRef.current = opsRef.current.filter(
      (op) => !(op.id === id && (opType === undefined || op.op === opType)),
    );
    if (opsRef.current.length !== before) forceRender();
  }, []);

  const removeById = useCallback((id: string) => {
    const before = opsRef.current.length;
    opsRef.current = opsRef.current.filter((op) => op.id !== id);
    if (opsRef.current.length !== before) forceRender();
  }, []);

  const failOp = useCallback((id: string, opType: PendingOp['op'], error: string) => {
    opsRef.current = opsRef.current.map((op) =>
      op.id === id && op.op === opType
        ? { ...op, status: 'failed' as const, error }
        : op,
    );
    forceRender();
  }, []);

  return {
    ops: opsRef.current,
    addOp,
    removeOp,
    removeById,
    failOp,
    forceRender,
    renderTick,
  };
}

// ── Merge helpers (re-export from pendingOps for backward compat) ─

import type { Entangled } from '@entangled/protocol';

/**
 * Merge confirmed server items with pending ops → Entangled<T>[] with _status metadata.
 * Used by useList. useStream uses its own simpler merge (ID dedup + append).
 */
export function mergeWithPending<T>(
  confirmed: T[],
  pending: PendingOp<T>[],
  getId: (item: T) => string,
): Entangled<T>[] {
  const result: Entangled<T>[] = confirmed.map(item => ({
    ...item,
    _status: 'confirmed' as const,
  }));

  for (const op of pending) {
    switch (op.op) {
      case 'create': {
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
          result[idx] = {
            ...result[idx],
            ...op.data,
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
        break;
      }
    }
  }

  return result;
}

/**
 * Merge server items with stream pending ops (append-only, ID dedup).
 * Used by useStream. Simpler than mergeWithPending — no update/delete ops.
 */
export function mergeStreamPending<T>(
  serverItems: T[],
  pending: PendingOp<T>[],
  getId: (item: T) => string,
): { items: T[]; pendingCleaned: PendingOp<T>[] } {
  if (pending.length === 0) return { items: serverItems, pendingCleaned: pending };

  const serverIds = new Set(serverItems.map(getId));
  const stillPending = pending.filter((p) => !serverIds.has(p.id));

  if (stillPending.length === 0) {
    return { items: serverItems, pendingCleaned: stillPending };
  }

  return {
    items: [...serverItems, ...stillPending.map((p) => p.data as T)],
    pendingCleaned: stillPending,
  };
}

// ── Generate unique request ID ──────────────────────────────────

let _counter = 0;
export function genRequestId(): string {
  return `rq_${Date.now()}_${++_counter}`;
}
