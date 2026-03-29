/**
 * @entangled/react — useList.ts
 *
 * Generic list hook with Entangled sync + optimistic state.
 *
 * Uses the unified useOptimisticOps engine for all optimistic state management.
 * Supports create/update/delete with requestId-based confirmation from delta pushes.
 */

import { useEffect, useMemo, useCallback } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { cacheGetList, entangledMethod } from './client';
import { subscribeWithCascade, unsubscribeWithCascade } from './subscriptionSchema';
import { useOptimisticOps, mergeWithPending, genRequestId } from './useOptimistic';
import type { Entangled } from '@entangled/protocol';
import type { QueryClient } from '@tanstack/react-query';
import { toSnakeParams } from './utils';

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
  invalidate: (client: QueryClient, params?: Record<string, string>) => void;
  buildKey: (params?: Record<string, string>) => string[];
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
    const paramsKey = useMemo(
      () => JSON.stringify(params),
      [...(def.keyParams ?? []).map((k) => params[k])],
    );
    const queryKey = useMemo(() => buildKey(params), [paramsKey]);
    const backendParams = useMemo(
      () => toSnakeParams(params, def.keyParams ?? []),
      [paramsKey],
    );
    const isEnabled = def.enabled ? def.enabled(params) : true;

    // ── Unified optimistic engine ───────────────────────────────
    const optimistic = useOptimisticOps<T>({
      entityName: def.name,
      enabled: isEnabled,
      confirmMode: 'requestId',
    });

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

    // ── Query: read from Rust SQLite cache ───────────────────────
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
      () => mergeWithPending(confirmedItems, optimistic.ops, def.getId),
      [confirmedItems, optimistic.renderTick],
    );

    // ── Create mutation ─────────────────────────────────────────
    const createMut = useMutation({
      mutationFn: async (data: any) => {
        const requestId = genRequestId();
        const tempId = `_tmp_${Date.now()}_${Math.random().toString(36).slice(2)}`;

        optimistic.addOp({
          id: tempId,
          requestId,
          op: 'create',
          data: { ...data, id: tempId },
          status: 'pending',
        });

        try {
          const result = await entangledMethod<T>(
            def.name,
            'create',
            { data, requestId },
            backendParams,
          );
          optimistic.removeById(tempId);
          return result;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          optimistic.failOp(tempId, 'create', msg);
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

        optimistic.addOp({
          id,
          requestId,
          op: 'update',
          data,
          status: 'pending',
        });

        try {
          const result = await entangledMethod<T>(
            def.name,
            'update',
            { id, data, requestId },
            backendParams,
          );
          optimistic.removeOp(id, 'update');
          return result;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          optimistic.failOp(id, 'update', msg);
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

        optimistic.addOp({
          id,
          requestId,
          op: 'delete',
          data: {},
          status: 'pending',
        });

        try {
          await entangledMethod(def.name, 'delete', { id, requestId }, backendParams);
          optimistic.removeOp(id, 'delete');
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          optimistic.failOp(id, 'delete', msg);
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

  function invalidate(client: QueryClient, params: Record<string, string> = {}) {
    const key = Object.keys(params).length > 0 ? buildKey(params) : [def.name];
    client.invalidateQueries({ queryKey: key });
  }

  return { name: def.name, useList, invalidate, buildKey };
}
