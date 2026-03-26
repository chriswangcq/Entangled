/**
 * @entangled/react — useStream.ts
 *
 * Generic stream (append-only) hook with Entangled sync.
 *
 * Subscribe uses head_n mode: initial sync gets latest N items,
 * then delta pushes append new items in real-time.
 * User can loadMore for backward pagination (older items).
 */

import { useEffect, useMemo } from 'react';
import {
  useInfiniteQuery, useMutation, useQueryClient,
  type InfiniteData,
} from '@tanstack/react-query';
import { subscribe, unsubscribe, cacheGetVersion, entityClient } from './client';
import type { StreamHookResult } from './types';

function toSnakeParams(
  params: Record<string, string>,
  keyParams: string[],
): Record<string, string> {
  const result: Record<string, string> = {};
  for (const k of keyParams) {
    if (params[k] !== undefined) {
      const snake = k.replace(/[A-Z]/g, (m) => `_${m.toLowerCase()}`);
      result[snake] = params[k];
    }
  }
  return result;
}

// ── Page shape ──────────────────────────────────────────────────

interface StreamPage<T> {
  items: T[];
  hasMore: boolean;
}

// ── Definition ──────────────────────────────────────────────────

export interface StreamDef<T> {
  name: string;
  keyParams: string[];
  getId: (item: T) => string;

  pageSize?: number;        // default: 50
  gcTime?: number;           // default: 10min
  depth?: number;            // initial sync depth (head_n), default: 50

  optimisticSend?: {
    createTemp: (data: any, params: Record<string, string>) => T;
  };

  enabled?: (params: Record<string, string>) => boolean;
}

export interface StreamStore<T> {
  name: string;
  useStream: (params: Record<string, string>) => StreamHookResult<T>;
  invalidate: (params?: Record<string, string>) => void;
}

// ── Factory ─────────────────────────────────────────────────────

export function createStreamStore<T>(def: StreamDef<T>): StreamStore<T> {
  const pageSize = def.pageSize ?? 50;

  function buildKey(params: Record<string, string>): string[] {
    return [def.name, ...def.keyParams.map((k) => params[k]).filter(Boolean)];
  }

  function useStream(params: Record<string, string>): StreamHookResult<T> {
    const qc = useQueryClient();
    const queryKey = useMemo(() => buildKey(params), [JSON.stringify(params)]);
    const backendParams = useMemo(() => toSnakeParams(params, def.keyParams), [JSON.stringify(params)]);
    const isEnabled = def.enabled ? def.enabled(params) : true;

    // ── Subscribe with depth (head_n) ───────────────────────────
    useEffect(() => {
      if (!isEnabled) return;

      let mounted = true;

      (async () => {
        const version = await cacheGetVersion(def.name, backendParams);
        if (!mounted) return;
        await subscribe(def.name, backendParams, {
          version,
          depth: def.depth ?? pageSize,
        });
      })();

      return () => {
        mounted = false;
        unsubscribe(def.name, backendParams);
      };
    }, [def.name, JSON.stringify(backendParams), isEnabled]);

    // ── Infinite query ──────────────────────────────────────────
    const query = useInfiniteQuery<StreamPage<T>>({
      queryKey,
      queryFn: async ({ pageParam }) => {
        if (!pageParam) {
          // First page: latest items (from sync or direct fetch)
          const items = await entityClient.list<T>(def.name, backendParams);
          return { items, hasMore: items.length >= pageSize };
        }
        // Backward pagination: older items
        const result = await entityClient.listStream<T>(def.name, {
          params: backendParams,
          id_lt: pageParam as string,
          limit: pageSize,
        });
        return { items: result.entries, hasMore: result.has_more };
      },
      initialPageParam: undefined as string | undefined,
      getNextPageParam: (lastPage) =>
        lastPage.hasMore && lastPage.items.length > 0
          ? def.getId(lastPage.items[0])
          : undefined,
      staleTime: Infinity,
      gcTime: def.gcTime ?? 10 * 60_000,
      enabled: isEnabled,
    });

    // ── Send mutation ───────────────────────────────────────────
    const sendMut = useMutation({
      mutationFn: (data: any) =>
        entityClient.create(def.name, data, backendParams),
      onMutate: def.optimisticSend
        ? async (data: any) => {
            const temp = def.optimisticSend!.createTemp(data, params);
            qc.setQueryData<InfiniteData<StreamPage<T>>>(
              queryKey,
              (old) => {
                if (!old?.pages?.length) return old;
                const firstPage = old.pages[0];
                return {
                  ...old,
                  pages: [
                    { ...firstPage, items: [...firstPage.items, temp] },
                    ...old.pages.slice(1),
                  ],
                };
              },
            );
            return { tempId: def.getId(temp) };
          }
        : undefined,
      onSettled: () => {
        qc.invalidateQueries({ queryKey });
      },
    });

    const allItems = query.data?.pages.flatMap((p) => p.items) ?? [];

    return {
      items: allItems,
      isLoading: query.isLoading,
      error: query.error,
      hasMore: query.hasNextPage ?? false,
      loadMore: () => { query.fetchNextPage(); },
      isLoadingMore: query.isFetchingNextPage,
      send: (data: any) => sendMut.mutateAsync(data),
      isSending: sendMut.isPending,
      refetch: () => { query.refetch(); },
    };
  }

  function invalidate(params: Record<string, string> = {}) {
    // Imperative invalidation
  }

  return { name: def.name, useStream, invalidate };
}
