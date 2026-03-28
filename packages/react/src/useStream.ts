/**
 * @entangled/react — useStream.ts
 *
 * Generic stream (append-only) hook with Entangled sync.
 *
 * ALL data flows through the Rust entity cache:
 *   - Live data: subscribe → server pushes delta → Rust cache updated → entities_changed → re-read
 *   - History:   loadMore → cachePrependPage (fetches via WS, writes into Rust cache) → re-read
 *
 * The React Query layer is a thin read-through cache on top of the Rust cache.
 * Pagination is lazy-triggered by the cache — when the UI calls loadMore(),
 * the entity engine fetches a page from the server and prepends it into the cache.
 */

import { useEffect, useMemo, useState, useCallback, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { genRequestId } from './pendingOps';
import {
  cacheGetList,
  cacheHasMore, cachePrependPage, entangledMethod,
} from './client';
import { subscribeWithCascade, unsubscribeWithCascade } from './subscriptionSchema';
import type { StreamHookResult } from './types';
import { globalQueryClient } from './syncListener';
import { toSnakeParams } from './utils';

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
    // Stable serialized key — avoids re-running JSON.stringify on every render
    const paramsKey = useMemo(() => JSON.stringify(params), [
      ...def.keyParams.map((k) => params[k]),
    ]);
    const queryKey = useMemo(() => buildKey(params), [paramsKey]);
    const backendParams = useMemo(() => toSnakeParams(params, def.keyParams), [paramsKey]);
    const isEnabled = def.enabled ? def.enabled(params) : true;

    // ── State: hasMore from Rust cache ───────────────────────────
    const [hasMore, setHasMore] = useState(false);
    const [isLoadingMore, setIsLoadingMore] = useState(false);
    const loadingMoreRef = useRef(false);

    // ── Subscribe with depth (head_n) ───────────────────────────
    useEffect(() => {
      if (!isEnabled) return;

      let mounted = true;

      (async () => {
        if (!mounted) return;
        await subscribeWithCascade(def.name, backendParams, {
          depth: def.depth ?? pageSize,
        });
        // After subscribe, check has_more from cache
        const more = await cacheHasMore(def.name, backendParams);
        if (mounted) setHasMore(more);
      })();

      return () => {
        mounted = false;
        void unsubscribeWithCascade(def.name, backendParams, {
          depth: def.depth ?? pageSize,
        });
      };
    }, [def.name, paramsKey, isEnabled]);

    // ── Query: read ALL items from Rust cache ────────────────────
    const query = useQuery<T[]>({
      queryKey,
      queryFn: async () => cacheGetList<T>(def.name, backendParams),
      staleTime: Infinity,   // Only invalidate via entities_changed
      gcTime: def.gcTime ?? 10 * 60_000,
      enabled: isEnabled,
    });

    // ── Load more: fetch older page → prepend into Rust cache ────
    const loadMore = useCallback(async () => {
      if (loadingMoreRef.current || !hasMore) return;
      const items = query.data;
      if (!items || items.length === 0) return;

      // Get the oldest item's ID as cursor
      const oldestId = def.getId(items[0]);
      loadingMoreRef.current = true;
      setIsLoadingMore(true);

      try {
        const result = await cachePrependPage(
          def.name, backendParams, oldestId, pageSize,
        );
        setHasMore(result.hasMore);
        // Invalidate query so it re-reads from the now-updated Rust cache
        qc.invalidateQueries({ queryKey });
      } catch (e) {
        console.error(`[useStream] loadMore failed for ${def.name}:`, e);
      } finally {
        loadingMoreRef.current = false;
        setIsLoadingMore(false);
      }
    }, [hasMore, query.data, backendParams, queryKey]);

    // ── Send mutation ───────────────────────────────────────────
    const sendMut = useMutation({
      mutationFn: async (data: any) => {
        const requestId = genRequestId();
        await entangledMethod(def.name, 'create', { data, requestId }, backendParams);
      },
      onSettled: () => {
        qc.invalidateQueries({ queryKey });
      },
    });

    return {
      items: query.data ?? [],
      isLoading: query.isLoading,
      error: query.error,
      hasMore,
      loadMore,
      isLoadingMore,
      send: (data: any) => sendMut.mutateAsync(data),
      isSending: sendMut.isPending,
      refetch: () => { query.refetch(); },
    };
  }

  function invalidate(params: Record<string, string> = {}) {
    const key = Object.keys(params).length > 0 ? buildKey(params) : [def.name];
    globalQueryClient?.invalidateQueries({ queryKey: key });
  }

  return { name: def.name, useStream, invalidate };
}
