/**
 * @entangled/react — useList.ts
 *
 * Generic list hook with Entangled sync + optimistic state.
 *
 * Data flow:
 *   1. Mount → subscribe(entity, params, version)
 *   2. Server sends sync frame → Rust cache applies → entities_changed emitted
 *   3. React reads from Rust cache (0 extra round-trip)
 *   4. Mutations: optimistic via pendingOps (survives invalidation)
 *   5. Delta push with requestId → confirms pendingOp
 *
 * Optimistic state:
 *   - pendingOps live in useRef, NOT in React Query cache
 *   - Each mutation generates a requestId that travels:
 *     WS request → Server → op_log → delta push → entities_changed → confirmByRequestIds
 *   - Components receive Entangled<T>[] with _status: 'confirmed' | 'pending' | 'failed'
 */

import { useEffect, useMemo, useRef, useState, useCallback } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import { cacheGetList, entangledMethod } from './client';
import { subscribeWithCascade, unsubscribeWithCascade } from './subscriptionSchema';
import {
  mergeWithPending, confirmByRequestIds, cleanupStaleOps, genRequestId,
  type PendingOp,
} from './pendingOps';
import type { Entangled } from '@entangled/protocol';
import { globalQueryClient } from './syncListener';

// ── camelCase → snake_case ──────────────────────────────────────

function toSnakeParams(
  params: Record<string, string>,
  keyParams?: string[],
): Record<string, string> {
  if (!keyParams) return params;
  const result: Record<string, string> = {};
  for (const k of keyParams) {
    if (params[k] !== undefined) {
      const snake = k.replace(/[A-Z]/g, (m) => `_${m.toLowerCase()}`);
      result[snake] = params[k];
    }
  }
  return result;
}

// ── Definition ──────────────────────────────────────────────────

export interface ListDef<T> {
  name: string;
  keyParams?: string[];
  getId: (item: T) => string;

  staleTime?: number;        // default: 30s
  gcTime?: number;           // default: 5min
  refetchOnFocus?: boolean;  // default: true

  enabled?: (params: Record<string, string>) => boolean;
}

export interface ListStore<T> {
  name: string;
  useList: (params?: Record<string, string>) => ListHookResult<T>;
  invalidate: (params?: Record<string, string>) => void;
}

export interface ListHookResult<T> {
  items: Entangled<T>[];
  isLoading: boolean;
  error: Error | null;
  refetch: () => void;
  create: (data: any) => Promise<T>;
  update: (id: string, data: any) => Promise<T>;
  remove: (id: string) => Promise<void>;
  isCreating: boolean;
  isUpdating: boolean;
  isRemoving: boolean;
}

// ── Factory ─────────────────────────────────────────────────────

export function createListStore<T>(def: ListDef<T>): ListStore<T> {

  function buildKey(params: Record<string, string> = {}): string[] {
    const suffix = def.keyParams?.map((k) => params[k]).filter(Boolean) ?? [];
    return suffix.length > 0 ? [def.name, ...suffix] : [def.name];
  }

  function useList(params: Record<string, string> = {}): ListHookResult<T> {
    const qc = useQueryClient();
    // Stable serialized key — avoids JSON.stringify on every render
    const paramsKey = useMemo(
      () => JSON.stringify(params),
      [...(def.keyParams ?? []).map((k) => params[k])],
    );
    const queryKey = useMemo(() => buildKey(params), [paramsKey]);
    const backendParams = useMemo(
      () => toSnakeParams(params, def.keyParams),
      [paramsKey],
    );
    const isEnabled = def.enabled ? def.enabled(params) : true;

    // ── Pending ops (survives invalidation) ─────────────────────
    const pendingOpsRef = useRef<PendingOp<T>[]>([]);
    const [renderTick, setRenderTick] = useState(0);
    const forceRender = useCallback(() => setRenderTick((n: number) => n + 1), []);

    // ── Listen for entities_changed with requestIds ─────────────
    useEffect(() => {
      if (!isEnabled) return;

      let unlisten: UnlistenFn | null = null;

      (async () => {
        unlisten = await listen<{ changes: Array<{ entity: string; requestIds?: string[] }> }>(
          'entities_changed',
          (event) => {
            for (const change of event.payload.changes) {
              if (change.entity !== def.name) continue;
              if (change.requestIds?.length) {
                const before = pendingOpsRef.current.length;
                pendingOpsRef.current = confirmByRequestIds(
                  pendingOpsRef.current,
                  change.requestIds,
                );
                if (pendingOpsRef.current.length !== before) {
                  forceRender();
                }
              }
            }
          },
        );
      })();

      return () => { unlisten?.(); };
    }, [def.name, isEnabled]);

    // ── Subscribe on mount ──────────────────────────────────────
    useEffect(() => {
      if (!isEnabled) return;

      let mounted = true;

      (async () => {
        if (!mounted) return;
        await subscribeWithCascade(def.name, backendParams, {});
      })();

      return () => {
        mounted = false;
        void unsubscribeWithCascade(def.name, backendParams, {});
      };
    }, [def.name, paramsKey, isEnabled]);

    // ── Timeout cleanup (30s safety net) ─────────────────────────
    useEffect(() => {
      const timer = setInterval(() => {
        const before = pendingOpsRef.current.length;
        pendingOpsRef.current = cleanupStaleOps(pendingOpsRef.current);
        if (pendingOpsRef.current.length !== before) forceRender();
      }, 5000);
      return () => clearInterval(timer);
    }, []);

    // ── Query: 只读 Rust SQLite（由 subscribe → sync 帧写入；eager 实体在 initialize 额外 bootstrap）
    const query = useQuery<T[]>({
      queryKey,
      queryFn: async () => cacheGetList<T>(def.name, backendParams),
      staleTime: def.staleTime ?? 30_000,
      gcTime: def.gcTime ?? 5 * 60_000,
      refetchOnWindowFocus: def.refetchOnFocus ?? true,
      enabled: isEnabled,
    });

    // ── Merge confirmed + pending ───────────────────────────────
    const confirmedItems = query.data ?? [];
    const items = useMemo(
      () => mergeWithPending(confirmedItems, pendingOpsRef.current, def.getId),
      [confirmedItems, renderTick],
    );

    // ── Create mutation (requestId on WS envelope matches delta requestIds) ──
    const createMut = useMutation({
      mutationFn: async (data: any) => {
        const requestId = genRequestId();
        const tempId = `_tmp_${Date.now()}_${Math.random().toString(36).slice(2)}`;

        pendingOpsRef.current = [
          ...pendingOpsRef.current,
          {
            id: tempId,
            requestId,
            op: 'create',
            data: { ...data, id: tempId },
            status: 'pending',
            startedAt: Date.now(),
          },
        ];
        forceRender();

        try {
          const result = await entangledMethod<T>(
            def.name,
            'create',
            { data, requestId },
            backendParams,
          );
          pendingOpsRef.current = pendingOpsRef.current.filter(
            (op: PendingOp<T>) => op.id !== tempId,
          );
          forceRender();
          return result;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          pendingOpsRef.current = pendingOpsRef.current.map((op: PendingOp<T>) =>
            op.id === tempId ? { ...op, status: 'failed' as const, error: msg } : op,
          );
          forceRender();
          throw err;
        }
      },
      onSettled: () => {
        qc.invalidateQueries({ queryKey });
      },
    });

    // ── Update mutation ─────────────────────────────────────────
    const updateMut = useMutation({
      mutationFn: async ({ id, data }: { id: string; data: any }) => {
        const requestId = genRequestId();

        pendingOpsRef.current = [
          ...pendingOpsRef.current,
          {
            id,
            requestId,
            op: 'update',
            data,
            status: 'pending',
            startedAt: Date.now(),
          },
        ];
        forceRender();

        try {
          const result = await entangledMethod<T>(
            def.name,
            'update',
            { id, data, requestId },
            backendParams,
          );
          pendingOpsRef.current = pendingOpsRef.current.filter(
            (op: PendingOp<T>) => !(op.id === id && op.op === 'update'),
          );
          forceRender();
          return result;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          pendingOpsRef.current = pendingOpsRef.current.map((op: PendingOp<T>) =>
            op.id === id && op.op === 'update'
              ? { ...op, status: 'failed' as const, error: msg }
              : op,
          );
          forceRender();
          throw err;
        }
      },
      onSettled: () => {
        qc.invalidateQueries({ queryKey });
      },
    });

    // ── Delete mutation ─────────────────────────────────────────
    const removeMut = useMutation({
      mutationFn: async (id: string) => {
        const requestId = genRequestId();

        pendingOpsRef.current = [
          ...pendingOpsRef.current,
          {
            id,
            requestId,
            op: 'delete',
            data: {},
            status: 'pending',
            startedAt: Date.now(),
          },
        ];
        forceRender();

        try {
          await entangledMethod(def.name, 'delete', { id, requestId }, backendParams);
          pendingOpsRef.current = pendingOpsRef.current.filter(
            (op: PendingOp<T>) => !(op.id === id && op.op === 'delete'),
          );
          forceRender();
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          pendingOpsRef.current = pendingOpsRef.current.map((op: PendingOp<T>) =>
            op.id === id && op.op === 'delete'
              ? { ...op, status: 'failed' as const, error: msg }
              : op,
          );
          forceRender();
          throw err;
        }
      },
      onSettled: () => {
        qc.invalidateQueries({ queryKey });
      },
    });

    return {
      items,
      isLoading: query.isLoading,
      error: query.error,
      refetch: () => { query.refetch(); },
      create: (data: any) => createMut.mutateAsync(data),
      update: (id: string, data: any) => updateMut.mutateAsync({ id, data }),
      remove: (id: string) => removeMut.mutateAsync(id),
      isCreating: createMut.isPending,
      isUpdating: updateMut.isPending,
      isRemoving: removeMut.isPending,
    };
  }

  function invalidate(params: Record<string, string> = {}) {
    const key = Object.keys(params).length > 0 ? buildKey(params) : [def.name];
    globalQueryClient?.invalidateQueries({ queryKey: key });
  }

  return { name: def.name, useList, invalidate };
}
