import type { ComponentPublicInstance, CSSProperties } from 'vue'

/** The pane's open size (percent of the splitter) until the user drags it. */
const DEFAULT_SIZE = 30
/** Smallest size the user can drag an open pane to. */
const MIN_SIZE = 15
const DURATION = 200

/** What the template refs bind to: a reka `SplitterPanel` instance. */
interface PaneHandle {
    resize: (size: number) => void
    getSize: () => number
}

/**
 * A splitter side pane that opens and closes by tweening its size, so the
 * neighbouring pane resizes in step with the slide instead of jumping. While
 * the size moves, the content keeps its open width anchored to the right
 * edge: it slides in and out rather than reflowing.
 *
 * Bind `pane` to the side `SplitterPanel` (rendered always, default size 0),
 * `group` to the `SplitterGroup` with `@layout="onLayout"`, and `contentStyle`
 * to the pane's absolutely positioned content wrapper.
 */
export function useSlidingPane(storageKey: string) {
    const pane = ref<PaneHandle | null>(null)
    const group = ref<HTMLElement | ComponentPublicInstance | null>(null)
    const { width: groupWidth } = useElementSize(group)

    /** Logically open: set by `show`, cleared by `hide` as the close starts. */
    const opened = ref(false)
    /** The content is mounted: the pane is open, opening or closing. */
    const mounted = ref(false)
    const animating = ref(false)
    const openSize = useLocalStorage(storageKey, DEFAULT_SIZE)

    let frame = 0
    function tween(from: number, to: number, done?: () => void) {
        cancelAnimationFrame(frame)
        animating.value = true
        const start = performance.now()
        const step = (now: number) => {
            const t = Math.min(1, (now - start) / DURATION)
            const eased = 1 - (1 - t) ** 3
            pane.value?.resize(from + (to - from) * eased)
            if (t < 1) {
                frame = requestAnimationFrame(step)
                return
            }
            animating.value = false
            done?.()
        }
        frame = requestAnimationFrame(step)
    }

    function show() {
        opened.value = true
        if (mounted.value && !animating.value) return
        mounted.value = true
        nextTick(() => tween(pane.value?.getSize() ?? 0, openSize.value))
    }

    function hide() {
        if (!opened.value) return
        opened.value = false
        const current = pane.value?.getSize() ?? 0
        if (!animating.value && current >= MIN_SIZE) openSize.value = current
        tween(current, 0, () => { mounted.value = false })
    }

    onBeforeUnmount(() => cancelAnimationFrame(frame))

    /**
     * The drag minimum, enforced here rather than as the panel's `min-size`: the
     * tween passes below it, and toggling that constraint makes the splitter
     * re-clamp a stale size (the pane snaps when an animation starts or ends).
     */
    function onLayout(sizes: number[]) {
        const size = sizes.at(-1) ?? 0
        if (opened.value && !animating.value && size < MIN_SIZE) pane.value?.resize(MIN_SIZE)
    }

    const contentStyle = computed<CSSProperties>(() => (animating.value
        ? { width: `${(groupWidth.value * openSize.value) / 100}px` }
        : { left: '0' }))

    return { pane, group, opened, mounted, animating, contentStyle, show, hide, onLayout }
}
