let probeCanvas: CanvasRenderingContext2D | null = null

/**
 * Resolve a CSS colour expression (a token, a `color-mix` of tokens) to an
 * `rgb()`/`rgba()` string a canvas library such as ECharts can parse. The
 * browser computes the expression, then a 1×1 canvas converts whatever colour
 * space it lands in (oklch, oklab) to sRGB bytes.
 */
export function resolveColor(expression: string): string {
    const probe = document.createElement('span')
    probe.style.color = expression
    document.body.appendChild(probe)
    const computed = getComputedStyle(probe).color
    probe.remove()

    probeCanvas ??= document.createElement('canvas').getContext('2d', { willReadFrequently: true })
    if (!probeCanvas) return computed
    probeCanvas.clearRect(0, 0, 1, 1)
    probeCanvas.fillStyle = computed
    probeCanvas.fillRect(0, 0, 1, 1)
    const [r = 0, g = 0, b = 0, a = 0] = probeCanvas.getImageData(0, 0, 1, 1).data
    return a === 255 ? `rgb(${r}, ${g}, ${b})` : `rgba(${r}, ${g}, ${b}, ${(a / 255).toFixed(3)})`
}

/**
 * The tokens a canvas chart draws with, resolved for the current colour mode:
 * the app's font, its text and line colours, and a tooltip styled like the
 * app's own. Recomputed after every colour-mode switch, once the `.dark` class
 * has landed.
 */
export function useChartTheme() {
    const colorMode = useColorMode()
    const revision = ref(0)
    watch(() => colorMode.value, () => nextTick(() => revision.value++))

    return computed(() => {
        void revision.value
        const fontFamily = getComputedStyle(document.documentElement).getPropertyValue('--font-sans').trim()
        const text = resolveColor('var(--ui-text-muted)')
        const ink = resolveColor('var(--ui-text-highlighted)')
        const line = resolveColor('var(--ui-border)')
        return {
            fontFamily,
            /** `text-xs`, the size of every chart label. */
            fontSize: 12,
            text,
            ink,
            line,
            surface: resolveColor('var(--ui-bg-muted)'),
            primary: resolveColor('var(--ui-primary)'),
            resolve: resolveColor,
            status: (color: StatusColor) => resolveColor(STATUS_CSS[color]),
            /** ECharts `tooltip` styling matching UTooltip. */
            tooltip: {
                backgroundColor: resolveColor('var(--ui-bg)'),
                borderColor: line,
                textStyle: { color: ink, fontFamily, fontSize: 12 },
            },
        }
    })
}
