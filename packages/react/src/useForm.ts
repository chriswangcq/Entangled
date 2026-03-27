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
import { subscribe, unsubscribe, cacheGetItem, cacheGetVersion, entityClient } from './client';
import type { FormHookResult } from './types';
import { globalQueryClient } from './syncListener';

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
  invalidate: (params?: Record<string, string>) => void;
}

function resolveEntityId(def: FormDef<any>, params: Record<string, string>): string {
  if (typeof def.entityId === 'function') return def.entityId(params);
  if (typeof def.entityId === 'string') return params[def.entityId] ?? '';
  const firstKey = def.keyParams?.[0];
  return firstKey ? (params[firstKey] ?? '') : '';
}

// ── Factory ─────────────────────────────────────────────────────

export function createFormStore<T>(def: FormDef<T>): FormStore<T> {

  function buildKey(params: Record<string, string> = {}): string[] {
    const suffix = def.keyParams?.map((k) => params[k]).filter(Boolean) ?? [];
    return suffix.length > 0 ? [def.name, ...suffix] : [def.name];
  }

  function useForm(params: Record<string, string> = {}): FormHookResult<T> {
    const qc = useQueryClient();
    const queryKey = useMemo(() => buildKey(params), [JSON.stringify(params)]);
    const backendParams = useMemo(() => toSnakeParams(params, def.keyParams), [JSON.stringify(params)]);
    const isEnabled = def.enabled ? def.enabled(params) : true;

    // ── Subscribe / Unsubscribe ─────────────────────────────────
    useEffect(() => {
      if (!isEnabled) return;

      let mounted = true;

      (async () => {
        const version = await cacheGetVersion(def.name, backendParams);
        if (!mounted) return;
        await subscribe(def.name, backendParams, { version });
      })();

      return () => {
        mounted = false;
        unsubscribe(def.name, backendParams);
      };
    }, [def.name, JSON.stringify(backendParams), isEnabled]);

    // ── Query ───────────────────────────────────────────────────
    const query = useQuery<T>({
      queryKey,
      queryFn: async () => {
        const id = resolveEntityId(def, params);
        return entityClient.get<T>(def.name, id, backendParams);
      },
      staleTime: def.staleTime ?? 30_000,
      gcTime: def.gcTime ?? 5 * 60_000,
      enabled: isEnabled,
    });

    // ── Submit mutation ─────────────────────────────────────────
    const submitMut = useMutation({
      mutationFn: async (data: Partial<T>) => {
        const id = resolveEntityId(def, params);
        return entityClient.upsert<T>(def.name, id, data as Record<string, unknown>, backendParams);
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

  function invalidate(params: Record<string, string> = {}) {
    // Imperative invalidation uses globalQueryClient
    const key = Object.keys(params).length > 0 ? buildKey(params) : [def.name];
    globalQueryClient?.invalidateQueries({ queryKey: key });
  }

  return { name: def.name, useForm, invalidate };
}
