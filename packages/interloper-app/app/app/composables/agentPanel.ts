const DEFAULT_WIDTH = 400
const MIN_WIDTH = 320
const MAX_WIDTH = 720

/**
 * State of the docked agent chat panel (pushes the app layout): open flag
 * plus a draggable width shared between the panel and the layout offset.
 */
export function useAgentPanel() {
    const open = useState('agent-panel-open', () => false)
    const width = useCookie<number>('agent-panel-width', { default: () => DEFAULT_WIDTH })
    const dragging = useState('agent-panel-dragging', () => false)

    /** Drag the panel's left edge — width follows the pointer until release. */
    function startResize() {
        dragging.value = true
        const previousCursor = document.body.style.cursor
        const previousUserSelect = document.body.style.userSelect
        document.body.style.cursor = 'ew-resize'
        document.body.style.userSelect = 'none'

        function onMove(e: MouseEvent | TouchEvent) {
            const x = 'touches' in e ? e.touches[0]?.clientX : e.clientX
            if (x == null) return
            width.value = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, Math.round(window.innerWidth - x)))
        }
        function onEnd() {
            dragging.value = false
            document.body.style.cursor = previousCursor
            document.body.style.userSelect = previousUserSelect
            window.removeEventListener('mousemove', onMove)
            window.removeEventListener('touchmove', onMove)
        }
        window.addEventListener('mousemove', onMove)
        window.addEventListener('touchmove', onMove)
        window.addEventListener('mouseup', onEnd, { once: true })
        window.addEventListener('touchend', onEnd, { once: true })
    }

    function resetWidth() {
        width.value = DEFAULT_WIDTH
    }

    return { open, width, dragging, startResize, resetWidth }
}
