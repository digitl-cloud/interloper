import type { ComputedRef, Ref } from 'vue'
import type { Run } from '~/types/run'
import type { Execution } from '~/types/execution'
import type { RunStats } from '~/composables/runStats'

export interface RunAttempt {
    run: Run
    stats: RunStats
}

/**
 * The attempts of a run's stack, oldest first, each with its own asset stats.
 *
 * A failed-scope retry only executes what had not yet succeeded, so an
 * attempt's executions are its own, not the stack's: a later attempt usually
 * has fewer. The current attempt reads the live executions it is handed, so its
 * figures move as the run does; the others read the counts their run carries.
 */
export function useRunAttempts(run: Ref<Run | null>, current: Ref<Execution[]>): ComputedRef<RunAttempt[]> {
    const runsStore = useRunsStore()

    const root = computed(() => run.value?.root_run_id ?? run.value?.id ?? null)
    const stack = computed(() => {
        const attempts = root.value ? runsStore.stacks[root.value] : undefined
        return attempts ? [...attempts].sort((a, b) => a.attempt - b.attempt) : []
    })

    // Only a failed attempt is ever followed by another, so a first attempt that
    // has not failed is a stack of one.
    watch(run, async (value) => {
        if (!value || !root.value || (value.attempt === 1 && value.status !== 'failed')) return
        await runsStore.loadStack(root.value)
    }, { immediate: true })

    return computed(() => stack.value.map((attempt) => {
        const counts = attempt.id === run.value?.id ? executionCounts(current.value) : (attempt.execution_counts ?? {})
        return { run: attempt, stats: runStats(attempt, counts) }
    }))
}
