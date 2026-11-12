/** The short form a UUID is shown in (runs, backfills, tokens): its first eight characters. */
export function shortId(id: string): string {
    return id.slice(0, 8)
}
