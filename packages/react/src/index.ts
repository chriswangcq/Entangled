/**
 * @entangled/react — React hooks for Entangled sync engine.
 *
 * Three data shapes:
 *   - List: bounded, mutable CRUD collection (agents, devices, todos)
 *   - Form: single object (config, preferences)
 *   - Stream: append-only, unbounded (messages, logs)
 *
 * Usage:
 *   import { createListStore, createFormStore, createStreamStore, startSyncListener } from '@entangled/react';
 *
 *   // Define stores
 *   export const todosStore = createListStore<Todo>({ name: 'todos', getId: (t) => t.id });
 *   export const settingsStore = createFormStore<Settings>({ name: 'settings' });
 *   export const messagesStore = createStreamStore<Message>({ name: 'messages', keyParams: ['agentId'], getId: (m) => m.id });
 *
 *   // Start sync listener (once, at app startup)
 *   startSyncListener(queryClient);
 *
 *   // Use in components
 *   const { items, create } = todosStore.useList({ projectId: 'p1' });
 *   const { data, submit } = settingsStore.useForm();
 *   const { items, send, loadMore } = messagesStore.useStream({ agentId: 'a1' });
 */

// Hooks
export { createListStore } from './useList';
export type { ListDef, ListStore } from './useList';

export { createFormStore } from './useForm';
export type { FormDef, FormStore } from './useForm';

export { createStreamStore } from './useStream';
export type { StreamDef, StreamStore } from './useStream';

// Sync listener
export { startSyncListener, stopSyncListener } from './syncListener';

// Client (for advanced usage)
export { entityClient, subscribe, unsubscribe, cacheGetList, cacheGetItem, cacheGetVersion } from './client';

// Types
export type { ListHookResult, FormHookResult, StreamHookResult } from './types';
