/**
 * @entangled/react — useForm.ts
 *
 * Generic form (single-object) hook with Entangled sync.
 *
 * Same lifecycle as useList: subscribe on mount, unsubscribe on unmount.
 * Data is a single object (e.g. agent-tools config, user preferences).
 */

import { useEffect, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { cacheGetItem, entangledMethod } from './client';
import { subscribeWithCascade, unsubscribeWithCascade } from './subscriptionSchema';
import type { FormHookResult } from './types';
import type { QueryClient } from '@tanstack/react-query';
import { genRequestId } from './pendingOps';

import { toSnakeParams } from './utils';

// ── Definition ──────────────────────────────────────────────────

export interface FormDef<T> {
  name: string;
  keyParams?: string[];

  /** How to resolve entity ID from params */
  entityId?: string | ((params: Record<string, string>) => string);

  staleTime?: number;
  gcTime?: number;

  optimistic?: boolean;  // default: false
  defaultValue?: T | (() => T);
  enabled?: (params: Record<string, string>) => boolean;
}

export interface FormStore<T> {
  name: string;
  useForm: (params?: Record<string, string>) => FormHookResult<T>;
  invalidate: (client: QueryClient, params?: Record<string, string>) => void;
  buildKey: (params?: Record<string, string>) => string[];
}

function resolveEntityId(def: FormDef<any>, params: Record<string, string>): string {
  if (typeof def.entityId === 'function') return def.entityId(params);
  if (typeof def.entityId === 'string') return params[def.entityId] ?? '';

  // Fallback: use keyParams[0] — deprecated, add explicit entityId to FormDef
  const firstKey = def.keyParams?.[0];
  if (firstKey) {
    if (typeof globalThis !== 'undefined' && (globalThis as any).__DEV__ !== false) {
      console.warn(
        `[Entangled] FormDef '${def.name}' has no explicit entityId — ` +
        `falling back to keyParams[0]='${firstKey}'. ` +
        `Add 'entityId: "${firstKey}"' to the FormDef to silence this warning.`,
      );
    }
    return params[firstKey] ?? '';
  }
  return '';
}

// ── Factory ─────────────────────────────────────────────────────

export function createFormStore<T>(def: FormDef<T>): FormStore<T> {

  function buildKey(params: Record<string, string> = {}): string[] {
    return [def.name, ...def.keyParams?.map((k) => params[k]).filter(Boolean) || []];
  }

  function useForm(params: Record<string, string> = {}): FormHookResult<T> {
    const qc = useQueryClient();
    const queryKey = useMemo(() => buildKey(params), [JSON.stringify(params)]);
    const backendParams = useMemo(() => toSnakeParams(params, def.keyParams ?? []), [JSON.stringify(params)]);
    const isEnabled = def.enabled ? def.enabled(params) : true;

    // ── Subscribe / Unsubscribe ─────────────────────────────────
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
    }, [def.name, JSON.stringify(backendParams), isEnabled]);

    // ── Query ───────────────────────────────────────────────────
    const query = useQuery<T>({
      queryKey,
      queryFn: async () => {
        const id = resolveEntityId(def, params);
        const row = await cacheGetItem<T>(def.name, id, backendParams);
        if (row != null) return row;
        if (def.defaultValue != null) {
          return typeof def.defaultValue === 'function'
            ? (def.defaultValue as () => T)()
            : def.defaultValue;
        }
        throw new Error(`${def.name} not in local cache yet`);
      },
      staleTime: def.staleTime ?? 30_000,
      gcTime: def.gcTime ?? 5 * 60_000,
      enabled: isEnabled,
    });

    // ── Submit mutation ─────────────────────────────────────────
    const submitMut = useMutation({
      mutationFn: async (data: Partial<T>) => {
        const id = resolveEntityId(def, params);
        const requestId = genRequestId();
        return entangledMethod<T>(def.name, 'upsert', {
          id,
          data: data as Record<string, unknown>,
          requestId,
        }, backendParams);
      },
      onMutate: def.optimistic
        ? async (newData: Partial<T>) => {
            await qc.cancelQueries({ queryKey });
            const prev = qc.getQueryData<T>(queryKey);
            qc.setQueryData<T>(queryKey, (old) =>
              old ? ({ ...old, ...newData } as T) : (newData as T),
            );
            return { prev };
          }
        : undefined,
      onError: def.optimistic
        ? (_e: any, _v: any, ctx: any) => {
            if (ctx?.prev !== undefined) qc.setQueryData(queryKey, ctx.prev);
          }
        : undefined,
      onSettled: () => {
        qc.invalidateQueries({ queryKey });
      },
    });

    const defaultVal = def.defaultValue
      ? typeof def.defaultValue === 'function'
        ? (def.defaultValue as () => T)()
        : def.defaultValue
      : null;

    return {
      data: query.data ?? defaultVal,
      isLoading: query.isLoading,
      error: query.error,
      refetch: () => { query.refetch(); },
      submit: (data: Partial<T>) => submitMut.mutateAsync(data),
      isSubmitting: submitMut.isPending,
    };
  }

  function invalidate(client: QueryClient, params: Record<string, string> = {}) {
    const key = Object.keys(params).length > 0 ? buildKey(params) : [def.name];
    client.invalidateQueries({ queryKey: key });
  }

  return { name: def.name, useForm, invalidate, buildKey };
}
