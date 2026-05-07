/**
 * Realtime WebSocket subscriptions backed by PostgreSQL NOTIFY.
 *
 * Usage in stores:
 *   useRealtimeSubscription({
 *       table: 'runs',
 *       scope: () => organisationStore.organisation?.id,
 *       onEnter: async () => { runs.value = await fetchRuns() },
 *       onLeave: () => { runs.value = [] },
 *       onInsert: (record) => upsertRun(record),
 *       onUpdate: (record) => upsertRun(record),
 *       onDelete: (record) => removeRun(record.id),
 *   })
 */

import type { MaybeRefOrGetter } from 'vue'

type RealtimeEvent = 'INSERT' | 'UPDATE' | 'DELETE'
type RealtimeHandler = (event: RealtimeEvent, record: Record<string, any>) => void

// ---------------------------------------------------------------------------
// Shared WebSocket connection (module-level singleton)
// ---------------------------------------------------------------------------

let _ws: WebSocket | null = null
let _reconnectTimer: ReturnType<typeof setTimeout> | null = null
let _reconnectDelay = 1000
const _MAX_RECONNECT_DELAY = 30000
const _handlers = new Map<string, Set<RealtimeHandler>>()
let _wsUrl: string | null = null

function _connect() {
    if (!_wsUrl) return
    if (_ws?.readyState === WebSocket.OPEN || _ws?.readyState === WebSocket.CONNECTING) return

    _ws = new WebSocket(_wsUrl)

    _ws.onopen = () => {
        _reconnectDelay = 1000
    }

    _ws.onmessage = (e) => {
        try {
            const msg = JSON.parse(e.data)
            const handlers = _handlers.get(msg.table)
            if (handlers) {
                for (const h of handlers) h(msg.event, msg.record)
            }
        }
        catch (err) {
            console.error('[Realtime] Failed to parse message', err)
        }
    }

    _ws.onclose = () => {
        _ws = null
        _reconnectTimer = setTimeout(_connect, _reconnectDelay)
        _reconnectDelay = Math.min(_reconnectDelay * 2, _MAX_RECONNECT_DELAY)
    }

    _ws.onerror = () => {
        _ws?.close()
    }
}

function _ensureConnection() {
    if (!_wsUrl) {
        const config = useRuntimeConfig()
        const devApiPort = config.public.devApiPort
        const base = devApiPort
            ? `ws://localhost:${devApiPort}`
            : window.location.origin.replace(/^http/, 'ws')
        _wsUrl = base + '/api/ws'
    }
    _connect()
}

function _subscribe(table: string, handler: RealtimeHandler): () => void {
    if (!_handlers.has(table)) _handlers.set(table, new Set())
    _handlers.get(table)!.add(handler)
    return () => {
        _handlers.get(table)?.delete(handler)
    }
}

// ---------------------------------------------------------------------------
// Public composable
// ---------------------------------------------------------------------------

interface UseRealtimeSubscriptionOptions {
    table: string
    scope: MaybeRefOrGetter<string | null | undefined>
    onEnter?: (scopeId: string) => void | Promise<void>
    onLeave?: () => void | Promise<void>
    shouldHandle?: (record: Record<string, any>) => boolean
    onInsert?: (record: Record<string, any>) => void
    onUpdate?: (record: Record<string, any>) => void
    onDelete?: (record: Record<string, any>) => void
    onAny?: (event: RealtimeEvent, record: Record<string, any>) => void
}

export function useRealtimeSubscription(options: UseRealtimeSubscriptionOptions) {
    _ensureConnection()

    let unsub: (() => void) | null = null
    let revision = 0

    watch(
        () => toValue(options.scope),
        async (scopeId) => {
            const currentRevision = ++revision

            unsub?.()
            unsub = null

            if (!scopeId) {
                await options.onLeave?.()
                return
            }

            await options.onEnter?.(scopeId)

            // Scope changed during async onEnter — bail
            if (currentRevision !== revision) return

            unsub = _subscribe(options.table, (event, record) => {
                if (options.shouldHandle && !options.shouldHandle(record)) return

                if (event === 'INSERT') options.onInsert?.(record)
                else if (event === 'UPDATE') options.onUpdate?.(record)
                else if (event === 'DELETE') options.onDelete?.(record)

                options.onAny?.(event, record)
            })
        },
        { immediate: true },
    )

    onScopeDispose(() => {
        unsub?.()
        unsub = null
    })
}
