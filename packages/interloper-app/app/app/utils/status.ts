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

export function statusLabel(status?: string | null): string {
    if (!status) return '—'
    return status.charAt(0).toUpperCase() + status.slice(1)
}
