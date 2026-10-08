import type { Ref } from 'vue'
import type { LocationQuery, LocationQueryValue, LocationQueryValueRaw, RouteLocationRaw, Router } from 'vue-router'

/** How a value reads from and writes to its query parameter. */
export interface QueryCodec<T> {
    /** The value a raw parameter holds, or undefined when it holds none (the parameter then reads as the fallback). */
    parse: (raw: string) => T | undefined
    format: (value: T) => string
}

const textQuery: QueryCodec<string> = { parse: raw => raw, format: value => value }

export const booleanQuery: QueryCodec<boolean> = {
    parse: raw => raw === 'true' ? true : raw === 'false' ? false : undefined,
    format: String,
}

/** A closed set of strings, so a stale or hand-edited link falls back instead of selecting nothing. */
export function oneOfQuery<T extends string>(values: readonly T[]): QueryCodec<T> {
    return { parse: raw => values.includes(raw as T) ? raw as T : undefined, format: value => value }
}

function firstValue(value: LocationQueryValue | LocationQueryValue[] | undefined): string | undefined {
    const first = Array.isArray(value) ? value[0] : value
    return typeof first === 'string' ? first : undefined
}

const pending = new Map<string, LocationQueryValueRaw>()

/** Writes from one tick land as a single navigation, so setting two parameters never drops one. */
function writeQuery(router: Router, name: string, value: string | undefined) {
    if (!pending.size) {
        nextTick(() => {
            const query = { ...router.currentRoute.value.query, ...Object.fromEntries(pending) }
            pending.clear()
            router.replace({ query })
        })
    }
    pending.set(name, value)
}

/**
 * A route query parameter as a ref, so a page's filters and views are
 * linkable. The ref answers at once and the URL follows, replacing the
 * history entry; a navigation that changes the parameter (a link to the
 * bare page, say) flows back into the ref. The fallback, and null, are
 * never written: an unfiltered page keeps a bare URL.
 *
 * @param name     The query parameter.
 * @param fallback The value an absent or unreadable parameter reads as.
 * @param codec    How the value reads from and writes to the parameter; plain text by default.
 */
export function useQueryParam(name: string, fallback: string): Ref<string>
export function useQueryParam(name: string, fallback: null): Ref<string | null>
export function useQueryParam<T>(name: string, fallback: NoInfer<T>, codec: QueryCodec<NonNullable<T>>): Ref<T>
export function useQueryParam<T>(name: string, fallback: T, codec: QueryCodec<any> = textQuery): Ref<T> {
    const route = useRoute()
    const router = useRouter()

    function read(): T {
        const raw = firstValue(route.query[name])
        return raw === undefined ? fallback : codec.parse(raw) ?? fallback
    }

    const value = ref(read()) as Ref<T>
    watch(() => route.query[name], () => { value.value = read() })
    watch(value, (next) => {
        const raw = next === fallback || next === null ? undefined : codec.format(next)
        if (raw !== firstValue(route.query[name])) writeQuery(router, name, raw)
    })
    return value
}

const lastQueries = reactive(new Map<string, LocationQuery>())

/**
 * Links that return a page as it was left: the current page records the
 * query it shows, and a link to a path it recorded carries that query back.
 *
 * @returns The link target for a path: the path with its last query; any other target as is.
 */
export function useLastQuery() {
    const route = useRoute()
    watch(() => route.fullPath, () => { lastQueries.set(route.path, route.query) }, { immediate: true })
    return (to: RouteLocationRaw): RouteLocationRaw => {
        if (typeof to !== 'string') return to
        const query = lastQueries.get(to)
        return query ? { path: to, query } : to
    }
}
