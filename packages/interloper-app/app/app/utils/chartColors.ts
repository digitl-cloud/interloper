/**
 * Palette values for canvas/ECharts contexts that can't use CSS variables:
 * Tailwind's status scales and the theme tokens in assets/css/main.css.
 */
export const CHART_STATUS_COLORS: Record<string, { light: string, dark: string }> = {
    success: { light: '#00bc7d', dark: '#00d492' }, // emerald-500 / emerald-400
    failed: { light: '#fb2c36', dark: '#ff6467' }, // red-500 / red-400
    running: { light: '#2d7df6', dark: '#5c9ef8' }, // blue-500 / blue-400
    canceled: { light: '#fe9a00', dark: '#ffb900' }, // amber-500 / amber-400
    default: { light: '#d4d4d4', dark: '#737373' }, // gray-300 / gray-500
}

export const CHART_AXIS_COLORS = {
    axis: { light: '#525252', dark: '#a3a3a3' }, // gray-600 / gray-400
    grid: { light: '#e5e5e5', dark: '#262626' }, // gray-200 / gray-800
    bar: { light: '#2d7df6', dark: '#5c9ef8' }, // blue-500 / blue-400
    surface: { light: '#fafafa', dark: '#1c1c1c' }, // --ui-bg-muted, the card tone
    ink: { light: '#0a0a0a', dark: '#fafafa' }, // gray-950 / gray-50
}
