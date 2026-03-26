/**
 * @entangled/protocol — Shared WS protocol types.
 *
 * This package defines the wire format between Server ↔ Client.
 * The sync model is Git-like: subscribe = clone/pull, delta = pack,
 * version/head = commit pointer.
 */

// ── Entity Schema (pushed from Server to Client on connect) ───────────────

export interface EntitySchema {
  /** Entity name, e.g. "todos" */
  name: string;
  /** Key params for scoping, e.g. ["project_id"] */
  keyParams: string[];
  /** Push events this entity subscribes to */
  pushEvents: string[];
  /** Sync mode hint: "list" (mutable CRUD) or "stream" (append-only) */
  syncType: 'list' | 'stream';
}

/**
 * Entity relation — server-side only, not pushed to clients.
 */
export interface EntityRelation {
  target: string;
  paramMap: Record<string, string>;
  onActions?: ('created' | 'updated' | 'deleted')[];
}

// ── Sync Operations (Git-like commits) ────────────────────────────────────

/** A single sync operation — like a Git commit */
export interface SyncOp {
  /** Monotonic version number (commit index) */
  version: number;
  /** Operation type */
  op: 'insert' | 'update' | 'delete' | 'invalidate';
  /** Entity item ID */
  id: string;
  /** Item data (null for delete) */
  data?: Record<string, unknown>;
  /** Timestamp */
  ts: number;
  /** Correlation ID — traces back to the WS request that caused this op */
  requestId?: string;
}

// ── Subscribe / Unsubscribe (Client → Server) ────────────────────────────

/** Client → Server: establish entanglement */
export interface SubscribeFrame {
  type: 'subscribe';
  entity: string;
  params?: Record<string, string>;
  /** Client's last known version (null = first subscribe, like git clone) */
  version?: number | null;
  /** Client's last known item ID — for streams (like git HEAD) */
  head?: string | null;
  /** Max items for initial sync (like git clone --depth) */
  depth?: number;
}

/** Client → Server: break entanglement */
export interface UnsubscribeFrame {
  type: 'unsubscribe';
  entity: string;
  params?: Record<string, string>;
}

// ── Sync Response (Server → Client) ──────────────────────────────────────

export type SyncMode = 'snapshot' | 'delta' | 'head_n' | 'up_to_date';

/** Server → Client: sync data */
export interface SyncFrame {
  type: 'sync';
  entity: string;
  params?: Record<string, string>;
  mode: SyncMode;
  /** Current server version after this sync */
  version: number;

  // ── snapshot / head_n mode ──
  /** Full data (snapshot or head_n) */
  data?: unknown[];
  /** Whether more items exist (head_n only) */
  hasMore?: boolean;
  /** Total count (head_n only, optional) */
  total?: number;

  // ── delta mode ──
  /** Base version these deltas apply to */
  baseVersion?: number;
  /** Ordered list of operations since baseVersion */
  ops?: SyncOp[];
}

// ── Entity CRUD Request/Response (unchanged) ──────────────────────────────

export type EntityOp = 'list' | 'list_all' | 'list_stream' | 'get' | 'create' | 'update' | 'upsert' | 'delete' | 'action';

export interface EntityRequest {
  op: EntityOp;
  entity: string;
  id?: string;
  params?: Record<string, string>;
  data?: Record<string, unknown>;
  id_gt?: string;
  id_lt?: string;
  limit?: number;
  action_name?: string;
}

export interface EntityResponse<T = unknown> {
  success: boolean;
  entries?: T[];
  data?: T;
  has_more?: boolean;
  error?: string;
}

// ── WS Frame Types ────────────────────────────────────────────────────────

export interface RequestFrame {
  type: 'request';
  request_id: string;
  action: 'entity';
  data: EntityRequest;
}

export interface ResponseFrame {
  type: 'response';
  request_id: string;
  data?: EntityResponse;
  error?: string;
}

export interface PushFrame {
  type: 'push';
  event: string;
  data?: unknown;
}

export interface SchemaPush {
  entities: EntitySchema[];
}

// ── Client-side types ─────────────────────────────────────────────────────

export interface EntitiesChangedEvent {
  changes: Array<{
    entity: string;
    action: string;
    params?: Record<string, string>;
    /** requestIds from ops in this delta — for optimistic confirmation */
    requestIds?: string[];
  }>;
}

// ── Optimistic state types ────────────────────────────────────────────────

/** Metadata attached to every item returned by Entangled hooks */
export interface EntangledMeta {
  _status: 'confirmed' | 'pending' | 'failed';
  _op?: 'create' | 'update' | 'delete';
  _tempId?: string;
  _error?: string;
  _retry?: () => void;
}

/** An item with Entangled status metadata */
export type Entangled<T> = T & EntangledMeta;
