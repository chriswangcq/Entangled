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
 * WS reconnect replay is handled in Rust (`resubscribe_all_entangled_wire`).
 */

import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import type { QueryClient } from '@tanstack/react-query';

interface EntityChange {
  entity: string;
  action: string;   // "synced" | "delta" | "invalidated"
  params?: Record<string, string>;
  requestIds?: string[];
}

interface EntitiesChangedPayload {
  changes: EntityChange[];
}

export let globalQueryClient: QueryClient | null = null;

let _unlisten: UnlistenFn | null = null;

/**
 * Start listening for sync updates from Rust engine.
 * Call once at app startup.
 */
export async function startSyncListener(queryClient: QueryClient): Promise<void> {
  globalQueryClient = queryClient;
  if (_unlisten) return;

  _unlisten = await listen<EntitiesChangedPayload>('entities_changed', (event) => {
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
        // Rust already sent subscribe(version=null). Refresh React Query from SQLite.
        queryClient.invalidateQueries({ queryKey });
      } else {
        // synced or delta — data already in Rust cache, just re-read
        queryClient.invalidateQueries({ queryKey, refetchType: 'active' });
      }
    }
  });

}

/**
 * Stop listening.
 */
export function stopSyncListener(): void {
  if (_unlisten) {
    _unlisten();
    _unlisten = null;
  }
}
