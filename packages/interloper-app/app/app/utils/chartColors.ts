/**
 * Design palette values for canvas/ECharts contexts that can't use CSS
 * variables. Mirrors the scales in assets/css/main.css — keep in sync.
 */
export const CHART_STATUS_COLORS: Record<string, { light: string, dark: string }> = {
    success: { light: '#1fa463', dark: '#45bc84' }, // green-500 / green-400
    failed: { light: '#e5484d', dark: '#ea686c' }, // red-500 / red-400
    running: { light: '#2d7df6', dark: '#5c9ef8' }, // blue-500 / blue-400
    canceled: { light: '#e69e2e', dark: '#e9ac46' }, // amber-500 / amber-400
    default: { light: '#d4d4d9', dark: '#6b6b70' }, // gray-400 / gray-800
}

export const CHART_AXIS_COLORS = {
    axis: { light: '#6b6b70', dark: '#9a9aa0' }, // gray-800 / gray-600
    grid: { light: '#e8e8ec', dark: '#3f3f44' }, // gray-300 / gray-900
    bar: { light: '#2d7df6', dark: '#5c9ef8' }, // blue-500 / blue-400
}
