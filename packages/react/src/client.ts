/**
 * @entangled/react — client.ts
 *
 * **Entangled Method（宪法）**：一切写操作均为 Method 调用（标准 create/update/delete/upsert
 * 与自定义 action 同一语义）；见仓库根目录 `Entangled/CONSTITUTION.md`。
 *
 * - **Reads** (`cacheGetList` / `cacheGetItem` / `entityClient.list` / `entityClient.get`):
 *   Rust SQLite only — populated by subscribe + sync frames (or prepend for streams).
 * - **Writes** (`create` / `update` / `delete` / `action` / …):
 *   AppBridge `gateway_ws_request` → Gateway; server applies changes → sync back to SQLite.
 */

import { invoke } from '@tauri-apps/api/core';
import type { EntityRequest, EntityResponse, EntangledMethodArgs } from '@entangled/protocol';

const WS_CONNECT_RETRY_MS = 400;
const WS_CONNECT_RETRY_MAX = 10;

/**
 * Substrings for transient AppBridge / WS failures — keep in sync with
 * `novaic-app/src-tauri/src/core/app_bridge.rs` (`send_request`, `load_more_stream`) and
 * `novaic-app/src/services/api.ts` (`devices.grouped` HTTP fallback).
 */
function isTransientBridgeDown(msg: string): boolean {
  return (
    msg.includes('WS not connected') ||
    msg.includes('WS request timeout') ||
    msg.includes('WS send failed') ||
    msg.includes('WS request serialization failed') ||
    msg.includes('Request cancelled') ||
    msg.includes('sink not available') ||
    msg.includes('load_more timeout') ||
    msg.includes('AppBridge not connected') ||
    msg.includes('AppBridge disconnected') ||
    msg.includes('pending WS request cancelled')
  );
}

/** Direct `invoke` paths (e.g. entangled_load_more) — same backoff as gateway_ws_request. */
async function invokeWithBridgeRetry<T>(fn: () => Promise<T>): Promise<T> {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (isTransientBridgeDown(msg) && attempt < WS_CONNECT_RETRY_MAX - 1) {
        attempt++;
        await new Promise((r) => setTimeout(r, WS_CONNECT_RETRY_MS));
        continue;
      }
      throw e;
    }
  }
}

/** Mutations and stream fetch — retry while AppBridge is reconnecting. */
async function wsRequestWithRetry<T = any>(
  req: EntityRequest,
  requestId?: string,
): Promise<EntityResponse<T>> {
  let attempt = 0;
  while (true) {
    try {
      return await invoke<EntityResponse<T>>('gateway_ws_request', {
        action: 'entity',
        path: null,
        data: req,
        request_id: requestId ?? null,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (isTransientBridgeDown(msg) && attempt < WS_CONNECT_RETRY_MAX - 1) {
        attempt++;
        await new Promise((r) => setTimeout(r, WS_CONNECT_RETRY_MS));
        continue;
      }
      return { success: false, error: msg };
    }
  }
}

// ── Entangled Method — sole write surface (see CONSTITUTION.md) ───────

/**
 * Invoke a single **Entangled Method** on `entity` (standard: `create` | `update` | `delete` | `upsert`;
 * any other `method` string is a custom action registered on `EntityDef.actions`).
 *
 * - `create` → `{ data }`
 * - `update` | `upsert` → `{ id, data }`
 * - `delete` → `{ id }`
 * - Custom → `{ payload }` (optional; also accepts `data` as alias for payload)
 */
export async function entangledMethod<T = unknown>(
  entity: string,
  method: string,
  args: EntangledMethodArgs = {},
  params?: Record<string, string>,
): Promise<T> {
  const rid = args.requestId;
  switch (method) {
    case 'create': {
      if (args.data === undefined) throw new Error('entangledMethod(create): `data` required');
      const resp = await wsRequestWithRetry<T>(
        { op: 'create', entity, data: args.data, params },
        rid,
      );
      if (!resp.success) throw new Error(resp.error || `Failed to create ${entity}`);
      return resp.data as T;
    }
    case 'update': {
      if (!args.id || args.data === undefined) throw new Error('entangledMethod(update): `id` and `data` required');
      const resp = await wsRequestWithRetry<T>(
        {
          op: 'update',
          entity,
          id: args.id,
          data: args.data,
          params,
        },
        rid,
      );
      if (!resp.success) throw new Error(resp.error || `Failed to update ${entity}/${args.id}`);
      return resp.data as T;
    }
    case 'delete': {
      if (!args.id) throw new Error('entangledMethod(delete): `id` required');
      const resp = await wsRequestWithRetry(
        { op: 'delete', entity, id: args.id, params },
        rid,
      );
      if (!resp.success) throw new Error(resp.error || `Failed to delete ${entity}/${args.id}`);
      return undefined as T;
    }
    case 'upsert': {
      if (!args.id || args.data === undefined) throw new Error('entangledMethod(upsert): `id` and `data` required');
      const resp = await wsRequestWithRetry<T>(
        {
          op: 'upsert',
          entity,
          id: args.id,
          data: args.data,
          params,
        },
        rid,
      );
      if (!resp.success) throw new Error(resp.error || `Failed to upsert ${entity}/${args.id}`);
      return resp.data as T;
    }
    default: {
      const body = args.payload ?? args.data;
      const resp = await wsRequestWithRetry<T>(
        {
          op: 'action',
          entity,
          action_name: method,
          data: body,
          params,
        },
        rid,
      );
      if (!resp.success) throw new Error(resp.error || `Failed to invoke ${entity}.${method}`);
      return resp.data as T;
    }
  }
}

// ── Subscribe / Unsubscribe ─────────────────────────────────────

/**
 * Best-effort subscribe (no bridge-down retry here): `syncListener` listens for
 * `app_bridge_connected` and `resubscribeAll` re-sends subscriptions after reconnect.
 */
export async function subscribe(
  entity: string,
  params?: Record<string, string>,
  options?: { version?: number | null; depth?: number },
): Promise<void> {
  try {
    await invoke('entangled_subscribe', {
      entity,
      params: params || undefined,
      version: options?.version ?? null,
      depth: options?.depth ?? null,
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
    await invoke('entangled_unsubscribe', {
      entity,
      params: params || undefined,
    });
  } catch (e) {
    console.warn('[Entangled] Unsubscribe failed:', entity, e);
  }
}

// ── Cache access (through Rust) — sole read path for UI data ────

/** Read list from Rust SQLite (only source for UI reads; populate via subscribe + sync). */
export async function cacheGetList<T>(
  entity: string,
  params?: Record<string, string>,
): Promise<T[]> {
  try {
    const r = await invoke<T[] | null>('entity_list', { entity, params });
    return Array.isArray(r) ? r : [];
  } catch {
    return [];
  }
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

/** Check if a stream entity has more older items (from Rust cache). */
export async function cacheHasMore(
  entity: string,
  params?: Record<string, string>,
): Promise<boolean> {
  return invoke<boolean>('entity_has_more', { entity, params });
}

/**
 * Stream backward pagination — fully encapsulated in Entangled protocol.
 *
 * The entire flow (WS load_more → server fetch → Rust cache prepend → emit event)
 * is handled by the `entangled_load_more` Tauri command. The React layer only
 * provides the cursor (oldest item ID) and receives { count, hasMore }.
 */
export async function cachePrependPage<T = any>(
  entity: string,
  params: Record<string, string>,
  idLt: string,
  limit: number,
): Promise<{ count: number; hasMore: boolean }> {
  return invokeWithBridgeRetry(() =>
    invoke<{ count: number; hasMore: boolean }>('entangled_load_more', {
      entity,
      params,
      beforeId: idLt,
      limit,
    }),
  );
}

// ── entityClient: legacy facade — reads = cache; writes = entangledMethod ─

export const entityClient = {
  /**
   * List — **Rust cache only** (same as `cacheGetList`). Does not hit the network.
   */
  async list<T = any>(entity: string, params?: Record<string, string>): Promise<T[]> {
    return cacheGetList<T>(entity, params);
  },

  /**
   * Stream fetch — goes through Entangled load_more protocol.
   * Fills cache via `cachePrependPage` in hooks; rarely call directly.
   */
  async listStream<T = any>(
    entity: string,
    args: {
      params?: Record<string, string>;
      id_lt?: string;
      limit?: number;
    },
  ): Promise<{ entries: T[]; has_more: boolean }> {
    const result = await invokeWithBridgeRetry(() =>
      invoke<{ count: number; hasMore: boolean }>('entangled_load_more', {
        entity,
        params: args.params,
        beforeId: args.id_lt,
        limit: args.limit ?? 50,
      }),
    );
    // After load_more, entries are already in cache — read them back
    const entries = await cacheGetList<T>(entity, args.params);
    return {
      entries,
      has_more: result.hasMore,
    };
  },

  /**
   * Get — **Rust cache only**. Throws if the row is not in SQLite (subscribe first).
   */
  async get<T = any>(entity: string, id: string, params?: Record<string, string>): Promise<T> {
    const row = await cacheGetItem<T>(entity, id, params);
    if (row == null) {
      throw new Error(`Not in local cache: ${entity}/${id}`);
    }
    return row;
  },

  async create<T = any>(
    entity: string,
    data: Record<string, unknown>,
    params?: Record<string, string>,
  ): Promise<T> {
    return entangledMethod<T>(entity, 'create', { data }, params);
  },

  async update<T = any>(
    entity: string,
    id: string,
    data: Record<string, unknown>,
    params?: Record<string, string>,
  ): Promise<T> {
    return entangledMethod<T>(entity, 'update', { id, data }, params);
  },

  async upsert<T = any>(
    entity: string,
    id: string,
    data: Record<string, unknown>,
    params?: Record<string, string>,
  ): Promise<T> {
    return entangledMethod<T>(entity, 'upsert', { id, data }, params);
  },

  async remove(entity: string, id: string, params?: Record<string, string>): Promise<void> {
    return entangledMethod(entity, 'delete', { id }, params);
  },

  async action<T = any>(
    entity: string,
    actionName: string,
    data?: Record<string, unknown>,
    params?: Record<string, string>,
  ): Promise<T> {
    return entangledMethod<T>(entity, actionName, { payload: data }, params);
  },
};
