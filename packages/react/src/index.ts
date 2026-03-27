/**
 * @entangled/react — React hooks for Entangled sync engine.
 *
 * Three data shapes:
 *   - List: bounded, mutable CRUD collection (agents, devices, todos)
 *   - Form: single object (config, preferences)
 *   - Stream: append-only, unbounded (messages, logs)
 *
 * All mutations return Entangled<T>[] with _status metadata:
 *   - 'confirmed': server-confirmed data
 *   - 'pending': optimistic, waiting for server
 *   - 'failed': mutation failed, has _error and _retry
 */

// Hooks
export { createListStore } from './useList';
export type { ListDef, ListStore, ListHookResult } from './useList';

export { createFormStore } from './useForm';
export type { FormDef, FormStore } from './useForm';

export { createStreamStore } from './useStream';
export type { StreamDef, StreamStore } from './useStream';

// Sync listener
export { startSyncListener, stopSyncListener } from './syncListener';

// Pending ops engine
export { mergeWithPending, confirmByRequestIds, cleanupStaleOps, genRequestId } from './pendingOps';
export type { PendingOp } from './pendingOps';

// Client (for advanced usage)
export { entityClient, subscribe, unsubscribe, cacheGetList, cacheGetItem, cacheGetVersion, cacheHasMore, cachePrependPage } from './client';

// Types (re-export from protocol)
export type { Entangled, EntangledMeta, EntitiesChangedEvent } from '@entangled/protocol';
export type { FormHookResult, StreamHookResult } from './types';
