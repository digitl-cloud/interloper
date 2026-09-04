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

const DAY = 86_400_000

/** Milliseconds elapsed since midnight of the display-timezone day containing `date`. */
export function millisSinceMidnight(date: Date): number {
    const parts = new Intl.DateTimeFormat('en-US', {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hourCycle: 'h23',
        timeZone: displayTimeZone,
    }).formatToParts(date)
    const part = (type: string) => Number(parts.find(p => p.type === type)?.value ?? 0)
    return ((part('hour') * 60 + part('minute')) * 60 + part('second')) * 1000 + date.getMilliseconds()
}

/** Epoch ms of midnight, in the display timezone, of the day containing `ms`. */
export function startOfDay(ms: number): number {
    let midnight = ms - millisSinceMidnight(new Date(ms))
    // On DST-transition days the wall clock and elapsed time disagree by the
    // shift, so the first pass lands an hour off; nudge towards the nearer midnight.
    const drift = millisSinceMidnight(new Date(midnight))
    if (drift) midnight += drift > DAY / 2 ? DAY - drift : -drift
    return midnight
}

/** Wall-clock label in the display timezone, e.g. "14:05" or "14:05:30" with `seconds`. */
export function formatClockTime(date: Date, seconds = false): string {
    return date.toLocaleTimeString(undefined, {
        hour: '2-digit',
        minute: '2-digit',
        ...(seconds ? { second: '2-digit' } : {}),
        timeZone: displayTimeZone,
    })
}

/** Short day label in the display timezone, e.g. "4 Feb". */
export function formatShortDay(date: Date): string {
    return date.toLocaleDateString(undefined, { day: 'numeric', month: 'short', timeZone: displayTimeZone })
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