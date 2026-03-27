/**
 * Gateway-driven subscription policy (GET /api/entangled/schema).
 * - subscriptionMode lazy | eager: eager names are also subscribed at app startup.
 * - subscriptionCascade: ref-counted in Rust (`entangled_subscribe_cascade`); TS keeps a copy for eager names / introspection.
 */

import { invoke } from '@tauri-apps/api/core';

export type SubscriptionMode = 'lazy' | 'eager';

/** One row from Entangled EntityDef.to_schema_dict() / Gateway get_schema(). */
export interface EntitySubscriptionSchema {
  name: string;
  keyParams: string[];
  pushEvents: string[];
  syncType: string;
  syncLimit?: number | null;
  subscriptionMode?: SubscriptionMode;
  subscriptionCascade?: string[];
}

const byName = new Map<string, EntitySubscriptionSchema>();

export function setSubscriptionSchema(rows: EntitySubscriptionSchema[]): void {
  byName.clear();
  for (const row of rows) {
    if (row?.name) byName.set(row.name, row);
  }
}

async function pushSchemaToRust(rows: unknown): Promise<void> {
  try {
    await invoke('entangled_set_subscription_schema', { rows });
  } catch {
    /* non-Tauri / tests */
  }
}

/** Fetch JSON (e.g. gateway_get → array) and register. Swallows errors → empty registry. */
export async function loadSubscriptionSchema(
  fetchSchema: () => Promise<unknown>,
): Promise<void> {
  try {
    const data = await fetchSchema();
    const rows = Array.isArray(data) ? (data as EntitySubscriptionSchema[]) : [];
    setSubscriptionSchema(rows);
    await pushSchemaToRust(data);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    const is404 = /\b404\b/i.test(msg) || msg.includes('Not Found');
    if (is404) {
      console.debug('[Entangled] subscription schema unavailable (404), using eager defaults');
    } else {
      console.warn('[Entangled] loadSubscriptionSchema failed:', e);
    }
    setSubscriptionSchema([]);
    await pushSchemaToRust([]);
  }
}

export function getSubscriptionSchema(name: string): EntitySubscriptionSchema | undefined {
  return byName.get(name);
}

export function getEagerEntityNames(): string[] {
  const out: string[] = [];
  for (const d of byName.values()) {
    if (d.subscriptionMode === 'eager') out.push(d.name);
  }
  return out;
}

/** Targets to subscribe after `entity` (same params as parent). */
export function getSubscriptionCascade(entity: string): string[] {
  const d = byName.get(entity);
  const c = d?.subscriptionCascade;
  return Array.isArray(c) ? c.filter((x) => typeof x === 'string' && x.length > 0) : [];
}

function paramsForInvoke(backendParams: Record<string, string>): Record<string, string> | null {
  return Object.keys(backendParams).length > 0 ? backendParams : null;
}

/**
 * subscribe(entity) then subscribe each cascade target with the same params.
 * Ref-counted in Rust; paired with `unsubscribeWithCascade`.
 */
export async function subscribeWithCascade(
  entity: string,
  backendParams: Record<string, string>,
  opts: { depth?: number },
): Promise<void> {
  await invoke('entangled_subscribe_cascade', {
    entity,
    params: paramsForInvoke(backendParams),
    depth: opts.depth ?? null,
  });
}

/**
 * Decrement refs for `entity` and its cascade targets; unsubscribe when ref hits zero.
 */
export async function unsubscribeWithCascade(
  entity: string,
  backendParams: Record<string, string>,
  _opts: { depth?: number },
): Promise<void> {
  void _opts;
  await invoke('entangled_unsubscribe_cascade', {
    entity,
    params: paramsForInvoke(backendParams),
  });
}

/**
 * Re-subscribe all active subscriptions after WS reconnect (Rust ledger).
 * Prefer letting `app_bridge_connected` replay automatically; kept for callers that need an explicit refresh.
 */
export async function resubscribeAll(): Promise<void> {
  try {
    await invoke('entangled_resubscribe_all_active');
  } catch {
    /* non-Tauri */
  }
}

/**
 * @deprecated Rust replays subscribe(version=null) on `invalidated` when the key is still active.
 */
export async function resubscribeEntity(_entity: string, _params?: Record<string, string>): Promise<void> {}
