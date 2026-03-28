/**
 * @entangled/react — syncListener.ts
 *
 * Listens for sync frames from the Rust engine and updates React Query cache.
 * Now also forwards requestIds for optimistic confirmation.
 *
 * When Rust receives a sync frame from the server, it:
 * 1. Applies the delta/snapshot to its local cache
 * 2. Emits an "entities_changed" event with { entity, action, requestIds }
 * 3. This listener picks it up and invalidates React Query
 * 4. The hook's own listener uses requestIds to confirm pendingOps
 *
 * Also listens for "app_bridge_connected" (Tauri → Gateway `/api/app/ws`, the
 * same socket used for Entangled subscribe + load_more + sync frames — there is
 * no separate browser-side Entangled WebSocket) to re-subscribe after reconnect.
 */

import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import type { QueryClient } from '@tanstack/react-query';
import { resubscribeAll, resubscribeEntity } from './subscriptionSchema';

interface EntityChange {
  entity: string;
  action: string;   // "synced" | "delta" | "invalidated"
  params?: Record<string, string>;
  requestIds?: string[];
}

interface EntitiesChangedPayload {
  changes: EntityChange[];
}


let _unlisten: UnlistenFn | null = null;
let _unlistenReconnect: UnlistenFn | null = null;
/** Dedupes React Strict Mode double-invoke: concurrent init awaits the same setup. */
let _listenerSetupPromise: Promise<void> | null = null;
/** Bumped in stopSyncListener so in-flight setup abandons before assigning listeners. */
let _listenerSetupGen = 0;

/**
 * Start listening for sync updates from Rust engine.
 * Call once at app startup.
 */
export async function startSyncListener(queryClient: QueryClient): Promise<void> {
  if (_unlisten) return;
  if (!_listenerSetupPromise) {
    const gen = _listenerSetupGen;
    _listenerSetupPromise = (async () => {
      const uEntities = await listen<EntitiesChangedPayload>('entities_changed', (event) => {
        const { changes } = event.payload;

        for (const change of changes) {
          const queryKey: string[] = [change.entity];
          if (change.params) {
            const sorted = Object.keys(change.params).sort();
            for (const k of sorted) {
              queryKey.push(change.params[k]);
            }
          }

          if (change.action === 'invalidated') {
            resubscribeEntity(change.entity, change.params).then(() => {
              queryClient.invalidateQueries({ queryKey });
            }).catch((e) => {
              console.warn('[Entangled] resubscribeEntity failed:', change.entity, e);
              queryClient.invalidateQueries({ queryKey });
            });
          } else {
            queryClient.invalidateQueries({ queryKey, refetchType: 'active' });
          }
        }
      });
      if (gen !== _listenerSetupGen) {
        uEntities();
        return;
      }
      _unlisten = uEntities;

      const uReconnect = await listen('app_bridge_connected', () => {
        console.info(
          '[Entangled] AppBridge reconnected (/api/app/ws) — re-subscribing active subscriptions',
        );
        resubscribeAll().catch((e) => {
          console.warn('[Entangled] resubscribeAll failed:', e);
        });
      });
      if (gen !== _listenerSetupGen) {
        uReconnect();
        _unlisten();
        _unlisten = null;
        return;
      }
      _unlistenReconnect = uReconnect;
    })();
  }
  await _listenerSetupPromise;
}

/**
 * Stop listening.
 */
export function stopSyncListener(): void {
  _listenerSetupGen += 1;
  _listenerSetupPromise = null;
  if (_unlisten) {
    _unlisten();
    _unlisten = null;
  }
  if (_unlistenReconnect) {
    _unlistenReconnect();
    _unlistenReconnect = null;
  }
}
