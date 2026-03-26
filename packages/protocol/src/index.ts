/**
 * @entangled/protocol — Shared WS protocol types.
 *
 * This package defines the wire format between Server ↔ Client.
 * Both the Python server and Rust/TS clients implement this protocol.
 */

// ── Entity Schema (pushed from Server to Client on connect) ───────────────

/**
 * Entity schema pushed to client after WS connect.
 *
 * The client only needs: name → push_events (to know which pushes map
 * to which entity). Relations and cascade are server-side business logic.
 */
export interface EntitySchema {
  /** Entity name, e.g. "todos" */
  name: string;
  /** Key params for scoping, e.g. ["project_id"] */
  keyParams: string[];
  /** Push events this entity subscribes to */
  pushEvents: string[];
}

/**
 * Entity relation — server-side only, not pushed to clients.
 * Kept here for reference / server-side TypeScript implementations.
 */
export interface EntityRelation {
  target: string;
  paramMap: Record<string, string>;
  onActions?: ('created' | 'updated' | 'deleted')[];
}

// ── WS Protocol Messages ──────────────────────────────────────────────────

/** Entity CRUD operation types */
export type EntityOp = 'list' | 'list_all' | 'list_stream' | 'get' | 'create' | 'update' | 'upsert' | 'delete' | 'action';

/** Client → Server: entity request */
export interface EntityRequest {
  op: EntityOp;
  entity: string;
  id?: string;
  params?: Record<string, string>;
  data?: Record<string, unknown>;
  // list_stream pagination
  id_gt?: string;
  id_lt?: string;
  limit?: number;
  // action
  action_name?: string;
}

/** Server → Client: entity response */
export interface EntityResponse<T = unknown> {
  success: boolean;
  entries?: T[];
  data?: T;
  has_more?: boolean;
  error?: string;
}

// ── WS Frame Types ────────────────────────────────────────────────────────

/** Client → Server frame */
export interface RequestFrame {
  type: 'request';
  request_id: string;
  action: 'entity';
  data: EntityRequest;
}

/** Server → Client frame */
export interface ResponseFrame {
  type: 'response';
  request_id: string;
  data?: EntityResponse;
  error?: string;
}

/** Server → Client push frame */
export interface PushFrame {
  type: 'push';
  event: string;
  data?: unknown;
}

/** Entity change push payload */
export interface EntityChangePush {
  entity: string;
  action: 'created' | 'updated' | 'deleted';
  entity_id?: string;
  params?: Record<string, string>;
  /** Optional inline data (avoids re-fetch) */
  data?: unknown;
}

/** Schema push payload (sent once after connect) */
export interface SchemaPush {
  entities: EntitySchema[];
}

// ── Client-side types ─────────────────────────────────────────────────────

/** Batched change notification from Engine → React */
export interface EntitiesChangedEvent {
  changes: Array<{
    entity: string;
    params?: Record<string, string>;
  }>;
}
