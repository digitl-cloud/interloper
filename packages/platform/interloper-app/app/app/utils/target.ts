import type { Run } from '~/types/run'
import type { Backfill } from '~/types/backfill'

/** A record carrying a server-resolved target (runs, backfills). */
type Targeted = Pick<Run | Backfill, 'component_kind' | 'component_key' | 'component_name'>

/**
 * Display label for a run/backfill target.
 *
 * The target's identity is resolved on the record itself, by the API and
 * the realtime trigger alike, so no component lookup (and no loading race)
 * is involved. The identity fields null together exactly when the target
 * was deleted.
 */
export function targetLabel(record: Targeted): string {
    if (record.component_key === null) return 'Deleted'
    return record.component_name ?? record.component_key
}

/**
 * Icon for a run/backfill target: the timeline's job glyph for jobs, the
 * catalog's brand icon for everything else, the deleted marker when the
 * target is gone.
 */
export function targetIcon(record: Targeted): string {
    const { icons } = useAppConfig()
    if (record.component_key === null) return icons.deleted
    return record.component_kind === 'job' ? icons.job : componentIcon(record.component_key)
}
