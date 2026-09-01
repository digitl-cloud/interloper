import type { Run } from '~/types/run'
import type { Backfill } from '~/types/backfill'

/** A record carrying a server-resolved target (runs, backfills). */
type Targeted = Pick<Run | Backfill, 'component_id' | 'component_key' | 'component_name'>

/**
 * Display label for a run/backfill target.
 *
 * The target's identity is resolved server-side on the record itself, so no
 * component lookup (and no loading race) is involved. `component_id` nulls
 * when the target is deleted; a set id with no name/key is a realtime
 * partial that the next fetch enriches.
 */
export function targetLabel(record: Targeted): string {
    if (!record.component_id) return 'Deleted'
    return record.component_name ?? record.component_key ?? '…'
}
