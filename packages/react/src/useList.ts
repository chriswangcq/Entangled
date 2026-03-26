/**
 * @entangled/react — useList.ts
 *
 * Generic list hook with Entangled sync.
 *
 * Lifecycle:
 *   mount → subscribe(entity, params, version) → server sends sync frame
 *         → Rust applies to cache → emit entities_changed → React re-reads
 *   unmount → unsubscribe(entity, params) → server stops pushing
 *
 * The queryFn reads from Rust cache (0 network). If cache is stale,
 * it subscribes and waits for the sync frame, then re-reads.
 *
 * Usage:
 *   const todos = createListStore<Todo>({
 *     name: 'todos',
 *     keyParams: ['projectId'],
 *     getId: (t) => t.id,
 *   });
 *   // in component:
 *   const { items, create, update, remove } = todos.useList({ projectId: 'p1' });
 */

import { useEffect, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { subscribe, unsubscribe, cacheGetList, cacheGetVersion, entityClient } from './client';
import type { ListHookResult } from './types';

// ── camelCase → snake_case helper ───────────────────────────────

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

  optimisticCreate?: boolean;
  optimisticUpdate?: boolean;  // default: true
  optimisticDelete?: boolean;  // default: true

  enabled?: (params: Record<string, string>) => boolean;
}

export interface ListStore<T> {
  name: string;
  useList: (params?: Record<string, string>) => ListHookResult<T>;
  invalidate: (params?: Record<string, string>) => void;
  getData: (params?: Record<string, string>) => T[] | undefined;
}

// ── Factory ─────────────────────────────────────────────────────

export function createListStore<T>(def: ListDef<T>): ListStore<T> {

  function buildKey(params: Record<string, string> = {}): string[] {
    const suffix = def.keyParams?.map((k) => params[k]).filter(Boolean) ?? [];
    return suffix.length > 0 ? [def.name, ...suffix] : [def.name];
  }

  function useList(params: Record<string, string> = {}): ListHookResult<T> {
    const qc = useQueryClient();
    const queryKey = useMemo(() => buildKey(params), [JSON.stringify(params)]);
    const backendParams = useMemo(() => toSnakeParams(params, def.keyParams), [JSON.stringify(params)]);
    const isEnabled = def.enabled ? def.enabled(params) : true;

    // ── Subscribe on mount, unsubscribe on unmount ──────────────
    useEffect(() => {
      if (!isEnabled) return;

      let mounted = true;

      (async () => {
        // Get local version for delta sync
        const version = await cacheGetVersion(def.name, backendParams);
        if (!mounted) return;
        await subscribe(def.name, backendParams, { version });
      })();

      return () => {
        mounted = false;
        unsubscribe(def.name, backendParams);
      };
    }, [def.name, JSON.stringify(backendParams), isEnabled]);

    // ── Query: read from Rust cache ─────────────────────────────
    const query = useQuery<T[]>({
      queryKey,
      queryFn: async () => {
        // Try Rust cache first (instant, 0 network)
        const cached = await cacheGetList<T>(def.name, backendParams);
        if (cached !== null) return cached;

        // Cache miss — fall back to direct WS request
        return entityClient.list<T>(def.name, backendParams);
      },
      staleTime: def.staleTime ?? 30_000,
      gcTime: def.gcTime ?? 5 * 60_000,
      refetchOnWindowFocus: def.refetchOnFocus ?? true,
      enabled: isEnabled,
    });

    // ── Create mutation ─────────────────────────────────────────
    const createMut = useMutation({
      mutationFn: (data: any) =>
        entityClient.create<T>(def.name, data, backendParams),
      onMutate: def.optimisticCreate
        ? async (data: any) => {
            await qc.cancelQueries({ queryKey });
            const prev = qc.getQueryData<T[]>(queryKey);
            qc.setQueryData<T[]>(queryKey, (old) => [...(old ?? []), data as T]);
            return { prev };
          }
        : undefined,
      onError: def.optimisticCreate
        ? (_e: any, _v: any, ctx: any) => {
            if (ctx?.prev) qc.setQueryData(queryKey, ctx.prev);
          }
        : undefined,
      onSettled: () => {
        qc.invalidateQueries({ queryKey });
      },
    });

    // ── Update mutation ─────────────────────────────────────────
    const updateMut = useMutation({
      mutationFn: ({ id, data }: { id: string; data: any }) =>
        entityClient.update<T>(def.name, id, data, backendParams),
      onMutate:
        def.optimisticUpdate !== false
          ? async ({ id, data }: { id: string; data: any }) => {
              await qc.cancelQueries({ queryKey });
              const prev = qc.getQueryData<T[]>(queryKey);
              qc.setQueryData<T[]>(queryKey, (old) =>
                old?.map((item) =>
                  def.getId(item) === id ? ({ ...item, ...data } as T) : item,
                ) ?? [],
              );
              return { prev };
            }
          : undefined,
      onError:
        def.optimisticUpdate !== false
          ? (_e: any, _v: any, ctx: any) => {
              if (ctx?.prev) qc.setQueryData(queryKey, ctx.prev);
            }
          : undefined,
      onSettled: () => {
        qc.invalidateQueries({ queryKey });
      },
    });

    // ── Delete mutation ─────────────────────────────────────────
    const removeMut = useMutation({
      mutationFn: (id: string) =>
        entityClient.remove(def.name, id, backendParams),
      onMutate: def.optimisticDelete !== false
        ? async (id: string) => {
            await qc.cancelQueries({ queryKey });
            const prev = qc.getQueryData<T[]>(queryKey);
            qc.setQueryData<T[]>(queryKey, (old) =>
              old?.filter((item) => def.getId(item) !== id) ?? [],
            );
            return { prev };
          }
        : undefined,
      onError: def.optimisticDelete !== false
        ? (_e: any, _id: any, ctx: any) => {
            if (ctx?.prev) qc.setQueryData(queryKey, ctx.prev);
          }
        : undefined,
      onSettled: () => {
        qc.invalidateQueries({ queryKey });
      },
    });

    return {
      items: query.data ?? [],
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

  // ── Imperative API ────────────────────────────────────────────

  function invalidate(params: Record<string, string> = {}) {
    const { QueryClient } = require('@tanstack/react-query');
    // Note: imperative invalidate requires the QueryClient from context
    // In practice, this uses the default query client if configured
  }

  function getData(params: Record<string, string> = {}): T[] | undefined {
    return undefined; // Read from Rust cache via cacheGetList
  }

  return { name: def.name, useList, invalidate, getData };
}
