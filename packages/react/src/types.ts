/**
 * @entangled/react — types.ts
 *
 * Shared types for hooks and store definitions.
 */

// ── Hook return types ───────────────────────────────────────────

export interface ListHookResult<T> {
  items: T[];
  isLoading: boolean;
  error: Error | null;
  refetch: () => void;
  create: (data: any) => Promise<T>;
  update: (id: string, data: any) => Promise<T>;
  remove: (id: string) => Promise<void>;
  isCreating: boolean;
  isUpdating: boolean;
  isRemoving: boolean;
}

export interface FormHookResult<T> {
  data: T | null;
  isLoading: boolean;
  error: Error | null;
  refetch: () => void;
  submit: (data: Partial<T>) => Promise<T>;
  isSubmitting: boolean;
}

export interface StreamHookResult<T> {
  items: T[];
  isLoading: boolean;
  error: Error | null;
  hasMore: boolean;
  loadMore: () => void;
  isLoadingMore: boolean;
  send: (data: any) => Promise<void>;
  isSending: boolean;
  refetch: () => void;
}
