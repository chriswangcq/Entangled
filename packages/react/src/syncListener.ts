/**
 * @entangled/react — syncListener.ts
 *
 * Listens for sync frames from the Rust engine and updates React Query cache.
 * This is the bridge between Rust push processing and React state.
 *
 * When Rust receives a sync frame from the server, it:
 * 1. Applies the delta/snapshot to its local cache
 * 2. Emits an "entities_changed" event
 * 3. This listener picks it up and invalidates the React Query
 *
 * For delta frames with inline data, React Query doesn't need to re-fetch —
 * the Rust cache already has the updated data.
 */

import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import type { QueryClient } from '@tanstack/react-query';

interface EntityChange {
  entity: string;
  action: string;   // "synced" | "delta" | "invalidated"
  params?: Record<string, string>;
}

interface EntitiesChangedPayload {
  changes: EntityChange[];
}

let _unlisten: UnlistenFn | null = null;

/**
 * Start listening for sync updates from Rust engine.
 * Call once at app startup.
 */
export async function startSyncListener(queryClient: QueryClient): Promise<void> {
  if (_unlisten) return; // Already listening

  _unlisten = await listen<EntitiesChangedPayload>('entities_changed', (event) => {
    const { changes } = event.payload;

    for (const change of changes) {
      // Build query key from entity + params
      const queryKey: string[] = [change.entity];
      if (change.params) {
        const sorted = Object.keys(change.params).sort();
        for (const k of sorted) {
          queryKey.push(change.params[k]);
        }
      }

      if (change.action === 'invalidated') {
        // Stale — force re-fetch (re-subscribe will happen)
        queryClient.invalidateQueries({ queryKey });
      } else {
        // synced or delta — data is already in Rust cache,
        // just trigger React to re-read from cache
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
