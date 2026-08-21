/**
 * Design palette values for canvas/ECharts contexts that can't use CSS
 * variables. Mirrors the scales in assets/css/main.css — keep in sync.
 */
export const CHART_STATUS_COLORS: Record<string, { light: string, dark: string }> = {
    success: { light: '#1fa463', dark: '#45bc84' }, // green-500 / green-400
    failed: { light: '#e5484d', dark: '#ea686c' }, // red-500 / red-400
    running: { light: '#2d7df6', dark: '#5c9ef8' }, // blue-500 / blue-400
    canceled: { light: '#e69e2e', dark: '#e9ac46' }, // amber-500 / amber-400
    default: { light: '#d4d4d8', dark: '#71717a' }, // gray-300 / gray-500
}

export const CHART_AXIS_COLORS = {
    axis: { light: '#52525b', dark: '#71717a' }, // gray-600 / gray-500
    grid: { light: '#e4e4e7', dark: '#3f3f46' }, // gray-200 / gray-700
    bar: { light: '#2d7df6', dark: '#5c9ef8' }, // blue-500 / blue-400
}
