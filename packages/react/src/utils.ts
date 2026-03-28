/**
 * @entangled/react — utils.ts
 *
 * Shared utilities for Entangled React hooks.
 */

/**
 * Convert camelCase params to snake_case, filtered by keyParams.
 *
 * Used by useList / useStream to align React-side camelCase params
 * (e.g. { agentId: "123" }) with Python-side snake_case keys
 * (e.g. { agent_id: "123" }).
 */
export function toSnakeParams(
  params: Record<string, string>,
  keyParams: string[],
): Record<string, string> {
  const result: Record<string, string> = {};
  for (const k of keyParams) {
    if (params[k] !== undefined) {
      const snake = k.replace(/[A-Z]/g, (m) => `_${m.toLowerCase()}`);
      result[snake] = params[k];
    }
  }
  return result;
}
