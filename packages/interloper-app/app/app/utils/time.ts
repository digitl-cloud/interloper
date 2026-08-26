/** The user's preferred display timezone (IANA name); undefined falls back to the browser's. */
let displayTimeZone: string | undefined

/** Set by the user store when the profile loads or changes; invalid names fall back to the browser's. */
export function setDisplayTimeZone(tz: string | null | undefined) {
    try {
        if (tz) new Intl.DateTimeFormat(undefined, { timeZone: tz })
        displayTimeZone = tz ?? undefined
    }
    catch {
        displayTimeZone = undefined
    }
}

export function timeSince(date: Date): string {
    const seconds = Math.floor((new Date().valueOf() - date.valueOf()) / 1000)
    let interval = seconds / 31536000

    if (interval > 1) {
        return Math.floor(interval) + " years"
    }
    interval = seconds / 2592000
    if (interval > 1) {
        return Math.floor(interval) + " months"
    }
    interval = seconds / 86400
    if (interval > 1) {
        return Math.floor(interval) + " days"
    }
    interval = seconds / 3600
    if (interval > 1) {
        return Math.floor(interval) + " hours"
    }
    interval = seconds / 60
    if (interval > 1) {
        return Math.floor(interval) + " minutes"
    }
    return Math.floor(seconds) + " seconds"
}

/** Date-only label, e.g. "4 Feb 2026". */
export function formatDay(value: string | Date | null | undefined): string {
    if (!value) return '—'
    const date = typeof value === 'string' ? new Date(value) : value
    return date.toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric', timeZone: displayTimeZone })
}

export function formatDate(value: string | Date | null | undefined) {
    if (!value) return ''
    const date = typeof value === 'string' ? new Date(value) : value
    const day = date.toLocaleString('en-US', { day: 'numeric', timeZone: displayTimeZone })
    const month = date.toLocaleString('en-US', { month: 'short', timeZone: displayTimeZone })
    const time = date.toLocaleString('en-GB', {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        timeZone: displayTimeZone,
    })

    return `${day} ${month}, ${time}`
}

export function formatElapsed(start?: string | Date | null, end?: string | Date | null) {
    if (!start) return ''

    const startDate = start instanceof Date ? start : new Date(start)
    const endDate = end ? (end instanceof Date ? end : new Date(end)) : new Date()

    const startMs = startDate.getTime()
    const endMs = endDate.getTime()
    const diff = Math.max(0, endMs - startMs)

    if (diff < 1000)
        return `${diff}ms`

    const hours = Math.floor(diff / 3600000)
    const minutes = Math.floor((diff % 3600000) / 60000)
    const seconds = Math.floor((diff % 60000) / 1000)

    let result = ''
    if (hours > 0) result += `${hours}h `
    if (minutes > 0 || hours > 0) result += `${minutes}m `
    result += `${seconds}s`
    return result.trim()
}