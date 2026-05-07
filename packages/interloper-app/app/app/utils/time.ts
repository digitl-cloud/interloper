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

export function formatDate(value: string | Date | null | undefined) {
    if (!value) return ''
    const date = typeof value === 'string' ? new Date(value) : value
    const day = date.getDate()
    const month = date.toLocaleDateString('en-US', { month: 'short' })
    const hours = date.getHours().toString().padStart(2, '0')
    const minutes = date.getMinutes().toString().padStart(2, '0')
    const seconds = date.getSeconds().toString().padStart(2, '0')

    return `${day} ${month}, ${hours}:${minutes}:${seconds}`
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