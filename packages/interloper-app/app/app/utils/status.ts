export function statusColor(status?: string | null): 'success' | 'error' | 'info' | 'warning' | 'neutral' {
    switch (status) {
        case 'success': return 'success'
        case 'failed': return 'error'
        case 'running': return 'info'
        case 'dispatched': return 'info'
        case 'queued': return 'neutral'
        case 'pending': return 'neutral'
        case 'canceled': return 'warning'
        default: return 'neutral'
    }
}

/** StatusPill palette for a status — its vocabulary has no `info`; running reads as the accent. */
export function statusPillColor(status?: string | null): 'success' | 'error' | 'warning' | 'neutral' | 'primary' {
    const color = statusColor(status)
    return color === 'info' ? 'primary' : color
}

export function statusIcon(status?: string | null): string {
    switch (status) {
        case 'success': return 'i-lucide-check'
        case 'failed': return 'i-lucide-x'
        case 'running': return 'i-lucide-loader-circle'
        case 'dispatched': return 'i-lucide-loader-circle'
        case 'queued': return 'i-lucide-clock'
        case 'pending': return 'i-lucide-clock'
        case 'canceled': return 'i-lucide-ban'
        default: return 'i-lucide-circle'
    }
}

export function statusLabel(status?: string | null): string {
    if (!status) return '—'
    return status.charAt(0).toUpperCase() + status.slice(1)
}
