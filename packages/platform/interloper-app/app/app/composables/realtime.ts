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
 *       onReconnect: () => reloadRuns(),
 *   })
 */

import type { MaybeRefOrGetter } from 'vue'

type RealtimeEvent = 'INSERT' | 'UPDATE' | 'DELETE'
type RealtimeHandler = (event: RealtimeEvent, record: Record<string, any>) => void

// ---------------------------------------------------------------------------
// Shared WebSocket connection (module-level singleton), one organisation at a time
// ---------------------------------------------------------------------------

let _ws: WebSocket | null = null
let _orgId: string | null = null
let _reconnectTimer: ReturnType<typeof setTimeout> | null = null
let _reconnectDelay = 1000
const _MAX_RECONNECT_DELAY = 30000
/** Closed by the server for a caller that is signed out (4001) or cannot act in the organisation (4004): retrying cannot help. */
const _FINAL_CLOSE_CODES = new Set([4001, 4004])
const _handlers = new Map<string, Set<RealtimeHandler>>()
const _reconnectHandlers = new Set<() => void>()
/** Whether a socket of the current organisation closed, a failed attempt included: the next open has messages to catch up on. */
let _dropped = false
let _following = false

function _url(orgId: string): string {
    const devApiPort = useRuntimeConfig().public.devApiPort
    // In dev the socket skips the Nuxt proxy and reaches the API on its own port, on the page's
    // own host: the session cookie belongs to that host, whatever its port.
    const base = devApiPort
        ? `ws://${window.location.hostname}:${devApiPort}`
        : window.location.origin.replace(/^http/, 'ws')
    return `${base}/api/organisations/${orgId}/ws`
}

function _connect() {
    if (!_orgId) return
    const socket = new WebSocket(_url(_orgId))
    _ws = socket

    socket.onopen = () => {
        _reconnectDelay = 1000
        if (_dropped) {
            _dropped = false
            for (const h of _reconnectHandlers) h()
        }
    }

    socket.onmessage = (e) => {
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

    socket.onclose = (e) => {
        if (_ws !== socket) return
        _ws = null
        if (_FINAL_CLOSE_CODES.has(e.code)) return
        _dropped = true
        _reconnectTimer = setTimeout(_connect, _reconnectDelay)
        _reconnectDelay = Math.min(_reconnectDelay * 2, _MAX_RECONNECT_DELAY)
    }

    socket.onerror = () => {
        socket.close()
    }
}

/** Point the socket at an organisation, closing the previous one's; `null` closes it. */
function _open(orgId: string | null) {
    if (orgId === _orgId) return
    _orgId = orgId
    _dropped = false
    if (_reconnectTimer) clearTimeout(_reconnectTimer)
    _reconnectTimer = null
    _reconnectDelay = 1000
    const previous = _ws
    _ws = null
    previous?.close()
    _connect()
}

/** The socket follows the active organisation for the app's lifetime, from the first subscription on. */
function _ensureConnection() {
    if (_following) return
    _following = true
    const organisationStore = useOrganisationStore()
    effectScope(true).run(() => {
        watch(() => organisationStore.organisation?.id ?? null, _open, { immediate: true })
    })
}

function _subscribe(table: string, handler: RealtimeHandler, onReconnect?: () => void): () => void {
    if (!_handlers.has(table)) _handlers.set(table, new Set())
    _handlers.get(table)!.add(handler)
    if (onReconnect) _reconnectHandlers.add(onReconnect)
    return () => {
        _handlers.get(table)?.delete(handler)
        if (onReconnect) _reconnectHandlers.delete(onReconnect)
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
    /** The socket opened again after a drop: messages sent while it was down never arrived. */
    onReconnect?: () => void
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
            }, options.onReconnect)
        },
        { immediate: true },
    )

    onScopeDispose(() => {
        unsub?.()
        unsub = null
    })
}
