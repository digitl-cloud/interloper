<script setup lang="ts">
/**
 * When something last ran and how it ended: a badge in the status's colour
 * with its icon and the relative time, linking to the run. Without the run's
 * id (state stamped before runs recorded it) it links to the target's runs.
 * The tooltip names the outcome, or `note` in its place, and the exact time.
 */
import type { ComponentRecord } from '~/types/component'

const props = defineProps<{
    /** A run, backfill or execution status from the status table. */
    status?: string | null
    at: string
    runId?: string | null
    /** The component that ran, for the link when `runId` is unknown. */
    target?: Pick<ComponentRecord, 'name' | 'key' | 'kind'>
    note?: string
}>()

const organisationStore = useOrganisationStore()
const meta = computed(() => statusMeta(props.status))
const date = computed(() => new Date(props.at))
const to = computed(() => {
    if (props.runId) return organisationStore.path(`/executions/runs/${props.runId}`)
    const { target } = props
    return { path: organisationStore.path('/executions/runs'), query: target ? { q: target.name ?? target.key, kind: target.kind } : {} }
})
</script>

<template>
    <UTooltip :text="`${note ?? meta.label} · ${formatDayTime(date)}`">
        <NuxtLink :to="to"
                  class="inline-flex rounded-md hover:opacity-75 focus-visible:outline-2 focus-visible:outline-primary"
                  @click.stop>
            <UBadge :color="meta.color"
                    :icon="statusIcon(status)"
                    :ui="{ leadingIcon: meta.executing ? 'animate-spin' : undefined }">
                <time :datetime="date.toISOString()">{{ relativeTime(date) }}</time>
            </UBadge>
        </NuxtLink>
    </UTooltip>
</template>
