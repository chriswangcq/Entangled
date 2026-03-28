/**
 * Gateway-driven subscription policy (GET /api/entangled/schema).
 * - subscriptionMode lazy | eager: eager names are also subscribed at app startup.
 * - Cascade is handled entirely server-side (ws_handler._cascade_targets).
 *   Client sends ONE subscribe; server fans out to cascade targets automatically.
 */

import { subscribe, unsubscribe, cacheGetVersion } from './client';

export type SubscriptionMode = 'lazy' | 'eager';

/** One row from Entangled EntityDef.to_schema_dict() / Gateway get_schema(). */
export interface EntitySubscriptionSchema {
  name: string;
  keyParams: string[];
  pushEvents: string[];
  syncType: string;
  syncLimit?: number | null;
  subscriptionMode?: SubscriptionMode;
  capabilities?: {
    listStream: boolean;
    existsBefore: boolean;
    upsert: boolean;
    actions: string[];
  };
}

const byName = new Map<string, EntitySubscriptionSchema>();

export function setSubscriptionSchema(rows: EntitySubscriptionSchema[]): void {
  byName.clear();
  for (const row of rows) {
    if (row?.name) byName.set(row.name, row);
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
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    const is404 = /\b404\b/i.test(msg) || msg.includes('Not Found');
    if (is404) {
      console.debug('[Entangled] subscription schema unavailable (404), using eager defaults');
    } else {
      console.warn('[Entangled] loadSubscriptionSchema failed:', e);
    }
    setSubscriptionSchema([]);
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

// ── Ref-counted subscribe / unsubscribe ─────────────────────────

/** Ref-count per (entity, params key) — multiple hooks may share the same entity. */
const subscribeRefCounts = new Map<string, number>();
/** Track subscription depth per key (for resubscription after reconnect). */
const subscribeDepths = new Map<string, number | undefined>();
/** Track backendParams per key (for resubscription after reconnect). */
const subscribeParams = new Map<string, { entity: string; params: Record<string, string> }>();

function subscriptionKey(entity: string, backendParams: Record<string, string>): string {
  const keys = Object.keys(backendParams).sort();
  const parts = keys.map((k) => `${k}=${backendParams[k]}`);
  return `${entity}\0${parts.join('&')}`;
}

async function acquireSubscribe(
  entity: string,
  backendParams: Record<string, string>,
  opts: { depth?: number },
): Promise<void> {
  const k = subscriptionKey(entity, backendParams);
  const prev = subscribeRefCounts.get(k) ?? 0;
  if (prev > 0) {
    subscribeRefCounts.set(k, prev + 1);
    return;
  }

  const version = await cacheGetVersion(entity, backendParams);
  await subscribe(entity, backendParams, { version, depth: opts.depth });
  subscribeRefCounts.set(k, 1);
  subscribeDepths.set(k, opts.depth);
  subscribeParams.set(k, { entity, params: backendParams });
}

async function releaseUnsubscribe(entity: string, backendParams: Record<string, string>): Promise<void> {
  const k = subscriptionKey(entity, backendParams);
  const prev = subscribeRefCounts.get(k) ?? 0;
  if (prev <= 0) return;
  const n = prev - 1;
  if (n <= 0) {
    subscribeRefCounts.delete(k);
    subscribeDepths.delete(k);
    subscribeParams.delete(k);
    await unsubscribe(entity, backendParams);
  } else {
    subscribeRefCounts.set(k, n);
  }
}

/**
 * Subscribe to entity.  Server-side cascade: the server automatically expands
 * cascade targets and sends back one sync frame per target.
 * Ref-counted: paired with `unsubscribeWithCascade`.
 */
export async function subscribeWithCascade(
  entity: string,
  backendParams: Record<string, string>,
  opts: { depth?: number },
): Promise<void> {
  await acquireSubscribe(entity, backendParams, opts);
}

/**
 * Decrement ref for entity; unsubscribe when ref hits zero.
 * Server will auto-unsubscribe cascade targets.
 */
export async function unsubscribeWithCascade(
  entity: string,
  backendParams: Record<string, string>,
  _opts: { depth?: number },
): Promise<void> {
  void _opts;
  await releaseUnsubscribe(entity, backendParams);
}

/**
 * Re-subscribe all active subscriptions (ref > 0) after WS reconnect.
 *
 * When the WS disconnects and reconnects, the server has lost all subscription state.
 * React hooks still have ref > 0, so `acquireSubscribe` would skip re-sending.
 * This function bypasses ref-counting and forces re-subscribe with the latest
 * cached version, enabling delta sync from where the client left off.
 */
export async function resubscribeAll(): Promise<void> {
  const entries = Array.from(subscribeParams.entries());
  let count = 0;

  for (const [k, { entity, params }] of entries) {
    const ref = subscribeRefCounts.get(k) ?? 0;
    if (ref <= 0) continue;

    const depth = subscribeDepths.get(k);
    const version = await cacheGetVersion(entity, params);
    await subscribe(entity, params, { version, depth });
    count++;
  }

  if (count > 0) {
    console.info(`[Entangled] Resubscribed ${count} active subscription(s) after reconnect`);
  }
}

/**
 * Force re-subscribe for a specific entity after delta version mismatch.
 *
 * When the Rust cache receives a delta with a version mismatch, it emits
 * "invalidated" and sets local version to 0. This function forces a re-subscribe
 * with version=null to request a fresh snapshot/head_n from the server.
 */
export async function resubscribeEntity(
  entity: string,
  params?: Record<string, string>,
): Promise<void> {
  const bp = params ?? {};
  const k = subscriptionKey(entity, bp);
  const ref = subscribeRefCounts.get(k) ?? 0;
  if (ref <= 0) return; // Not actively subscribed

  const depth = subscribeDepths.get(k);
  // Send version=null to force full re-sync (not delta from stale version)
  await subscribe(entity, bp, { version: null, depth });
  console.debug(`[Entangled] Forced re-subscribe for ${entity} (invalidated)`);
}
