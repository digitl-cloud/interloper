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

/** Dates and times read day-first in every browser ("4 Feb 2026", "14:05"), matching `formatDate`. */
const DISPLAY_LOCALE = 'en-GB'

/**
 * Wall-clock label in the display timezone, on a 24-hour clock: "14:05",
 * "14:05:30" with `seconds`, "14:05:30.123" with `'ms'`.
 */
export function formatClockTime(date: Date, seconds: boolean | 'ms' = false): string {
    return date.toLocaleTimeString(DISPLAY_LOCALE, {
        hour: '2-digit',
        minute: '2-digit',
        hourCycle: 'h23',
        ...(seconds ? { second: '2-digit' } : {}),
        ...(seconds === 'ms' ? { fractionalSecondDigits: 3 } : {}),
        timeZone: displayTimeZone,
    })
}

/** Short day label in the display timezone, e.g. "4 Feb". */
export function formatShortDay(date: Date): string {
    return date.toLocaleDateString(DISPLAY_LOCALE, { day: 'numeric', month: 'short', timeZone: displayTimeZone })
}

/** Short day and wall-clock time in the display timezone, e.g. "4 Feb 14:05". */
export function formatDayTime(value: string | number | Date): string {
    const date = value instanceof Date ? value : new Date(value)
    return `${formatShortDay(date)} ${formatClockTime(date)}`
}

/**
 * A calendar date (`YYYY-MM-DD`, such as a partition's) as a label, e.g.
 * "Tue 4 Feb 2026". It names a day, not an instant, so no time zone shifts it.
 */
export function formatCalendarDay(isoDate: string, options: { weekday?: boolean, year?: boolean } = {}): string {
    const { weekday = false, year = true } = options
    return new Date(`${isoDate}T00:00:00Z`).toLocaleDateString(DISPLAY_LOCALE, {
        weekday: weekday ? 'short' : undefined,
        day: 'numeric',
        month: 'short',
        year: year ? 'numeric' : undefined,
        timeZone: 'UTC',
    })
}

/** Time elapsed since `date` in words, e.g. "3 hours"; prefer `relativeTime` for compact labels. */
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
    return date.toLocaleDateString(DISPLAY_LOCALE, { day: 'numeric', month: 'short', year: 'numeric', timeZone: displayTimeZone })
}

/** Day, month and 24-hour time with seconds in the display timezone, e.g. "4 Feb, 14:05:30"; empty for no value. */
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

/** Duration between two instants (or until now), e.g. "1h 4m 12s" or "850ms"; empty without a start. */
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

/** Compact relative label: "in 4h", "2h ago", "tomorrow", "in 3d". */
export function relativeTime(date: Date, now: Date = new Date()): string {
    const diff = date.getTime() - now.getTime()
    const abs = Math.abs(diff)
    const future = diff >= 0
    if (abs < 60_000) return future ? 'now' : 'just now'
    const minutes = Math.round(abs / 60_000)
    const hours = Math.round(abs / 3_600_000)
    if (abs >= DAY && future && startOfDay(date.getTime()) === startOfDay(now.getTime() + DAY)) return 'tomorrow'
    const label = abs < 3_600_000 ? `${minutes}m` : abs < DAY ? `${hours}h` : `${Math.round(abs / DAY)}d`
    return future ? `in ${label}` : `${label} ago`
}
