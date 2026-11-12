import type { InjectionKey } from 'vue'

const DEFAULT_WIDTH = 400
const MIN_WIDTH = 320
const MAX_WIDTH = 720

/**
 * State of the docked agent chat panel (the page below the navbar makes room
 * for it): open flag plus a draggable width shared between the panel and the
 * layout's `--agent-offset`.
 */
export function useAgentPanel() {
    const open = useState('agent-panel-open', () => false)
    // The live width is shared state; the cookie only remembers it across visits and is written
    // once a drag ends. Nuxt echoes every cookie write back to each of its refs asynchronously,
    // so a cookie written on every pointer move snaps the width back to values already left behind.
    const savedWidth = useCookie<number>('agent-panel-width', { default: () => DEFAULT_WIDTH })
    const width = useState('agent-panel-width', () => savedWidth.value)
    const dragging = useState('agent-panel-dragging', () => false)

    /**
     * Drag the handle in the gutter: width follows the pointer until release.
     * `inset` is the card's distance from the window's right edge (`--agent-inset`);
     * the gutter between the page and the card is as wide, the handle sits in its middle.
     */
    function startResize(inset: number) {
        dragging.value = true
        const previousCursor = document.body.style.cursor
        const previousUserSelect = document.body.style.userSelect
        document.body.style.cursor = 'ew-resize'
        document.body.style.userSelect = 'none'

        function onMove(e: MouseEvent | TouchEvent) {
            const x = 'touches' in e ? e.touches[0]?.clientX : e.clientX
            if (x == null) return
            width.value = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, Math.round(window.innerWidth - inset * 1.5 - x)))
        }
        function onEnd() {
            dragging.value = false
            savedWidth.value = width.value
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
        savedWidth.value = DEFAULT_WIDTH
    }

    return { open, width, dragging, startResize, resetWidth }
}

/** Provided by the layout that mounts the agent panel, so navbars only offer the toggle where it works. */
export const AGENT_PANEL_HOST: InjectionKey<boolean> = Symbol('agent-panel-host')
