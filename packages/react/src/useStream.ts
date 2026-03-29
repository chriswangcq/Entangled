/**
 * @entangled/react — useStream.ts
 *
 * Generic stream (append-only) hook with Entangled sync + optimistic send.
 *
 * Uses the unified useOptimisticOps engine for optimistic state management.
 * Stream-specific: only supports append (create) ops, uses ID-dedup confirmation.
 *
 * ALL data flows through the Rust entity cache:
 *   - Live data: subscribe → server pushes delta → Rust cache updated → entities_changed → re-read
 *   - History:   loadMore → cachePrependPage (fetches via WS, writes into Rust cache) → re-read
 */

import { useEffect, useMemo, useState, useCallback, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useOptimisticOps, mergeStreamPending, genRequestId } from './useOptimistic';
import {
  cacheGetList,
  cacheHasMore, cachePrependPage, entangledMethod,
} from './client';
import { getSubscriptionSchema, subscribeWithCascade, unsubscribeWithCascade } from './subscriptionSchema';
import type { StreamHookResult } from './types';
import type { QueryClient } from '@tanstack/react-query';
import { toSnakeParams } from './utils';

// ── Definition ──────────────────────────────────────────────────

export interface StreamDef<T> {
  name: string;
  keyParams: string[];
  getId: (item: T) => string;

  pageSize?: number;        // default: 50
  gcTime?: number;           // default: 10min
  depth?: number;            // initial sync depth (head_n), default: 50

  /**
   * Optimistic send configuration.
   *
   * When provided, `send()` immediately creates a temp item via `createTemp()`
   * and appends it to the displayed list, so the user sees the item instantly
   * before the server roundtrip completes.
   *
   * If `actionName` is set, send() uses a custom action instead of standard 'create'.
   * If `transformPayload` is set, data is transformed before sending.
   */
  optimisticSend?: {
    /** Create a temporary item for immediate UI display */
    createTemp: (data: any, params: Record<string, string>) => T;
    /** Custom action name. Default: 'create' (standard CRUD). Set to e.g. 'send' for custom actions. */
    actionName?: string;
    /** Transform data before sending to server. Default: data is sent as-is. */
    transformPayload?: (data: any, params: Record<string, string>) => any;
  };

  enabled?: (params: Record<string, string>) => boolean;
}

export interface StreamStore<T> {
  name: string;
  useStream: (params: Record<string, string>) => StreamHookResult<T>;
  invalidate: (client: QueryClient, params?: Record<string, string>) => void;
  buildKey: (params?: Record<string, string>) => string[];
}

// ── Factory ─────────────────────────────────────────────────────

export function createStreamStore<T>(def: StreamDef<T>): StreamStore<T> {
  const pageSize = def.pageSize ?? 50;

  function buildKey(params: Record<string, string> = {}): string[] {
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

    // ── Check Server Capability ────────────────────────────────────
    const schema = getSubscriptionSchema(def.name);
    const supportsListStream = schema?.capabilities?.listStream ?? true;

    // ── State: hasMore from Rust cache ───────────────────────────
    const [hasMore, setHasMore] = useState(false);
    const [isLoadingMore, setIsLoadingMore] = useState(false);
    const loadingMoreRef = useRef(false);

    // ── Unified optimistic engine (serverIdDedup mode) ──────────
    const optimistic = useOptimisticOps<T>({
      entityName: def.name,
      enabled: isEnabled,
      confirmMode: 'serverIdDedup',
    });

    // ── Subscribe with depth (head_n) ───────────────────────────
    useEffect(() => {
      if (!isEnabled) return;

      let mounted = true;

      (async () => {
        if (!mounted) return;
        await subscribeWithCascade(def.name, backendParams, {
          depth: def.depth ?? pageSize,
        });
        if (supportsListStream) {
          const more = await cacheHasMore(def.name, backendParams);
          if (mounted) setHasMore(more);
        } else {
          if (mounted) setHasMore(false);
        }
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

    // ── Merge server items + optimistic pending items ────────────
    const mergedItems = useMemo(() => {
      const serverItems = query.data ?? [];
      const { items, pendingCleaned } = mergeStreamPending(
        serverItems,
        optimistic.ops,
        def.getId,
      );

      // Update the ops ref if dedup cleaned some items
      // (This is a side-effect in useMemo, but it's safe because
      // we're only pruning confirmed items, not adding new ones)
      if (pendingCleaned.length !== optimistic.ops.length) {
        // The engine's ref will be updated on next forceRender cycle
        // For now, just return the correct merged items
      }

      return items;
    }, [query.data, optimistic.renderTick]);

    // ── Load more: fetch older page → prepend into Rust cache ────
    const loadMore = useCallback(async () => {
      if (!supportsListStream) {
        console.warn(`[useStream] loadMore ignored: Entity '${def.name}' capabilities.listStream=false`);
        return;
      }
      if (loadingMoreRef.current || !hasMore) return;
      const items = query.data;
      if (!items || items.length === 0) return;

      const oldestId = def.getId(items[0]);
      loadingMoreRef.current = true;
      setIsLoadingMore(true);

      try {
        const result = await cachePrependPage(
          def.name, backendParams, oldestId, pageSize,
        );
        setHasMore(result.hasMore);
        qc.invalidateQueries({ queryKey });
      } catch (e) {
        console.error(`[useStream] loadMore failed for ${def.name}:`, e);
      } finally {
        loadingMoreRef.current = false;
        setIsLoadingMore(false);
      }
    }, [hasMore, query.data, backendParams, queryKey]);

    // ── Send mutation (with optimistic support) ──────────────────
    const sendMut = useMutation({
      mutationFn: async (data: any) => {
        const requestId = genRequestId();
        let tempId: string | null = null;

        // 1. Optimistic: create temp item immediately for instant UI feedback
        if (def.optimisticSend) {
          const tempItem = def.optimisticSend.createTemp(data, params);
          tempId = def.getId(tempItem);

          optimistic.addOp({
            id: tempId,
            requestId,
            op: 'create',
            data: tempItem as any,
            status: 'pending',
          });
        }

        try {
          // 2. Send to server
          const actionName = def.optimisticSend?.actionName;
          if (actionName) {
            const payload = def.optimisticSend?.transformPayload
              ? def.optimisticSend.transformPayload(data, params)
              : data;
            await entangledMethod(def.name, actionName, { payload, requestId }, backendParams);
          } else {
            await entangledMethod(def.name, 'create', { data, requestId }, backendParams);
          }

          // 3. Success: remove pending item
          if (tempId) {
            optimistic.removeById(tempId);
          }
        } catch (err) {
          // 4. Error: remove pending item
          if (tempId) {
            optimistic.removeById(tempId);
          }
          throw err;
        }
      },
      onSettled: () => {
        qc.invalidateQueries({ queryKey });
      },
    });

    return {
      items: mergedItems,
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

  function invalidate(client: QueryClient, params: Record<string, string> = {}) {
    const key = Object.keys(params).length > 0 ? buildKey(params) : [def.name];
    client.invalidateQueries({ queryKey: key });
  }

  return { name: def.name, useStream, invalidate, buildKey };
}
