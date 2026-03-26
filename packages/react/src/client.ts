/**
 * @entangled/react — client.ts
 *
 * Entangled WS client — subscribes, receives sync frames,
 * and forwards CRUD requests through Tauri IPC.
 *
 * Key difference from NovAIC's entityClient:
 * - subscribe/unsubscribe lifecycle (mount→subscribe, unmount→unsubscribe)
 * - Sync frames (snapshot/delta) applied to Rust cache, not re-fetch
 */

import { invoke } from '@tauri-apps/api/core';
import type { EntityRequest, EntityResponse, SyncFrame } from '@entangled/protocol';

// ── WS Request (through Rust AppBridge) ─────────────────────────

async function wsRequest<T = any>(req: EntityRequest): Promise<EntityResponse<T>> {
  try {
    return await invoke<EntityResponse<T>>('gateway_ws_request', {
      action: 'entity',
      path: null,
      data: req,
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { success: false, error: msg };
  }
}

// ── Subscribe / Unsubscribe ─────────────────────────────────────

/** Send subscribe message to server. */
export async function subscribe(
  entity: string,
  params?: Record<string, string>,
  options?: { version?: number | null; depth?: number },
): Promise<void> {
  try {
    await invoke('gateway_ws_send', {
      data: {
        type: 'subscribe',
        entity,
        params: params || undefined,
        version: options?.version ?? null,
        depth: options?.depth,
      },
    });
  } catch (e) {
    console.warn('[Entangled] Subscribe failed:', entity, e);
  }
}

/** Send unsubscribe message to server. */
export async function unsubscribe(
  entity: string,
  params?: Record<string, string>,
): Promise<void> {
  try {
    await invoke('gateway_ws_send', {
      data: {
        type: 'unsubscribe',
        entity,
        params: params || undefined,
      },
    });
  } catch (e) {
    console.warn('[Entangled] Unsubscribe failed:', entity, e);
  }
}

// ── Cache access (through Rust) ─────────────────────────────────

/** Read list from Rust cache. Returns null if stale/missing. */
export async function cacheGetList<T>(
  entity: string,
  params?: Record<string, string>,
): Promise<T[] | null> {
  return invoke<T[] | null>('entity_list', { entity, params });
}

/** Read single item from Rust cache. */
export async function cacheGetItem<T>(
  entity: string,
  id: string,
  params?: Record<string, string>,
): Promise<T | null> {
  return invoke<T | null>('entity_get', { entity, id, params });
}

/** Get current local version. */
export async function cacheGetVersion(
  entity: string,
  params?: Record<string, string>,
): Promise<number | null> {
  return invoke<number | null>('entity_version', { entity, params });
}

// ── Entity CRUD client ──────────────────────────────────────────

export const entityClient = {
  async list<T = any>(entity: string, params?: Record<string, string>): Promise<T[]> {
    const resp = await wsRequest<T>({ op: 'list', entity, params });
    if (!resp.success) throw new Error(resp.error || `Failed to list ${entity}`);
    return resp.entries ?? [];
  },

  async get<T = any>(entity: string, id: string, params?: Record<string, string>): Promise<T> {
    const resp = await wsRequest<T>({ op: 'get', entity, id, params });
    if (!resp.success) throw new Error(resp.error || `Failed to get ${entity}/${id}`);
    return resp.data!;
  },

  async create<T = any>(entity: string, data: Record<string, unknown>, params?: Record<string, string>): Promise<T> {
    const resp = await wsRequest<T>({ op: 'create', entity, data, params });
    if (!resp.success) throw new Error(resp.error || `Failed to create ${entity}`);
    return resp.data!;
  },

  async update<T = any>(entity: string, id: string, data: Record<string, unknown>, params?: Record<string, string>): Promise<T> {
    const resp = await wsRequest<T>({ op: 'update', entity, id, data, params });
    if (!resp.success) throw new Error(resp.error || `Failed to update ${entity}/${id}`);
    return resp.data!;
  },

  async upsert<T = any>(entity: string, id: string, data: Record<string, unknown>, params?: Record<string, string>): Promise<T> {
    const resp = await wsRequest<T>({ op: 'upsert', entity, id, data, params });
    if (!resp.success) throw new Error(resp.error || `Failed to upsert ${entity}/${id}`);
    return resp.data!;
  },

  async remove(entity: string, id: string, params?: Record<string, string>): Promise<void> {
    const resp = await wsRequest({ op: 'delete', entity, id, params });
    if (!resp.success) throw new Error(resp.error || `Failed to delete ${entity}/${id}`);
  },

  async action<T = any>(entity: string, actionName: string, data?: Record<string, unknown>, params?: Record<string, string>): Promise<T> {
    const resp = await wsRequest<T>({ op: 'action', entity, action_name: actionName, data, params });
    if (!resp.success) throw new Error(resp.error || `Failed to action ${entity}.${actionName}`);
    return resp.data!;
  },
};
