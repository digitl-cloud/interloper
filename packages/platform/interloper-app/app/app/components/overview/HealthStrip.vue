<script setup lang="ts">
import type { Overview } from '~/types/overview'

const organisationStore = useOrganisationStore()

const props = defineProps<{ overview: Overview | null }>()

const overviewStore = useOverviewStore()
const { error } = storeToRefs(overviewStore)

const SPARK_HEIGHT = 32
const SUCCESS = STATUS_CSS[statusMeta('success').color]
const FAILED = STATUS_CSS[statusMeta('failed').color]

/** One bar per hour, oldest first: the failed runs stacked on the successful ones, an empty hour a hairline on the track. */
const spark = computed(() => {
    const hourly = props.overview?.runs.hourly ?? []
    const max = Math.max(1, ...hourly.map(b => b.succeeded + b.failed))
    return hourly.map((bucket, i) => ({
        key: bucket.hour,
        tooltip: `${23 - i}h ago · ${bucket.succeeded} succeeded · ${bucket.failed} failed`,
        ok: bucket.succeeded ? Math.max(2, Math.round(SPARK_HEIGHT * bucket.succeeded / max)) : 0,
        fail: bucket.failed ? Math.max(2, Math.round(SPARK_HEIGHT * bucket.failed / max)) : 0,
    }))
})

const activity = computed(() => {
    const { running, queued, longest_running_seconds } = props.overview?.activity
        ?? { running: 0, queued: 0, longest_running_seconds: null }
    return {
        running,
        queued,
        segments: [
            { value: running, color: STATUS_CSS[statusMeta('running').color] },
            { value: queued, color: STATUS_CSS[statusMeta('queued').color] },
        ],
        longest: longest_running_seconds === null ? null : formatElapsed(new Date(0), new Date(longest_running_seconds * 1000)),
    }
})

const backfillPct = computed(() => {
    const { partitions_done, partitions_total } = props.overview?.backfills ?? { partitions_done: 0, partitions_total: 0 }
    return partitions_total ? Math.round((100 * partitions_done) / partitions_total) : 0
})

const jobs = computed(() => {
    const { enabled, failing } = props.overview?.jobs ?? { enabled: 0, failing: 0 }
    return {
        enabled,
        failing,
        healthy: enabled - failing,
        segments: [{ value: enabled - failing, color: SUCCESS }, { value: failing, color: FAILED }],
    }
})
</script>

<template>
    <LoadError v-if="!overview && error"
               title="Couldn't load the overview"
               :error="error"
               @retry="overviewStore.fetchOverview()" />
    <div v-else
         class="grid grid-cols-2 gap-4 sm:gap-6 xl:grid-cols-4">
        <OverviewHealthTile label="Runs · last 24h"
                            :to="organisationStore.path('/executions/runs')"
                            :loading="!overview"
                            :headline="overview?.runs.total"
                            :empty="overview?.runs.total ? undefined : 'No runs in the last 24h'">
            <template #detail>
                {{ overview?.runs.succeeded }} succeeded ·
                <ULink :to="{ path: organisationStore.path('/executions/runs'), query: { status: 'failed' } }"
                       class="relative hover:underline"
                       :class="overview?.runs.failed ? 'font-medium text-error' : 'text-muted'">{{ overview?.runs.failed }} failed</ULink>
            </template>
            <template #footer>
                <div class="flex items-end gap-0.5"
                     :style="{ height: `${SPARK_HEIGHT}px` }">
                    <UTooltip v-for="bar in spark"
                              :key="bar.key"
                              :text="bar.tooltip"
                              :delay-duration="0">
                        <div class="flex h-full flex-1 flex-col justify-end gap-px">
                            <div v-if="bar.fail"
                                 class="rounded-full"
                                 :style="{ height: `${bar.fail}px`, backgroundColor: FAILED }" />
                            <div v-if="bar.ok"
                                 class="rounded-full"
                                 :style="{ height: `${bar.ok}px`, backgroundColor: SUCCESS }" />
                            <div v-if="!bar.ok && !bar.fail"
                                 class="h-px rounded-full bg-accented" />
                        </div>
                    </UTooltip>
                </div>
                <div class="flex justify-between text-xs text-dimmed"><span>24h ago</span><span>Now</span></div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Running now"
                            :to="{ path: organisationStore.path('/executions/runs'), query: { status: 'running' } }"
                            :loading="!overview"
                            :headline="activity.running"
                            :empty="activity.running + activity.queued ? undefined : 'Nothing running or queued'">
            <template #detail>
                <ULink :to="{ path: organisationStore.path('/executions/runs'), query: { status: 'queued' } }"
                       class="relative text-muted hover:underline">{{ activity.queued }} queued</ULink>
            </template>
            <template #footer>
                <UProgressGroup :items="activity.segments"
                                :max="activity.running + activity.queued" />
                <div class="flex justify-between text-xs tabular-nums text-dimmed">
                    <span>{{ activity.running }} running · {{ activity.queued }} queued</span>
                    <span v-if="activity.longest">Longest {{ activity.longest }}</span>
                </div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Backfills in progress"
                            :to="{ path: organisationStore.path('/executions/backfills'), query: { status: 'running', single: 'true' } }"
                            :loading="!overview"
                            :headline="overview?.backfills.active"
                            :empty="overview?.backfills.active ? undefined : 'No backfills in progress'">
            <template v-if="overview?.backfills.active"
                      #detail>
                <span class="tabular-nums">{{ overview.backfills.partitions_done }} of {{ overview.backfills.partitions_total }} partitions</span>
            </template>
            <template #footer>
                <UProgress :model-value="backfillPct" />
                <div class="flex justify-between text-xs text-dimmed">
                    <span>Combined progress</span>
                    <span class="font-semibold tabular-nums text-primary">{{ backfillPct }}%</span>
                </div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Jobs failing"
                            :to="organisationStore.path(kindPath('job'))"
                            :loading="!overview"
                            :headline="jobs.failing"
                            :headline-class="jobs.failing ? 'text-error' : ''"
                            :empty="overview?.jobs.enabled ? undefined : 'No jobs enabled'">
            <template #detail>of {{ overview?.jobs.enabled }} enabled</template>
            <template #footer>
                <UProgressGroup :items="jobs.segments"
                                :max="jobs.enabled" />
                <div class="flex justify-between text-xs tabular-nums text-dimmed">
                    <span>{{ jobs.healthy }} healthy</span>
                    <span>{{ jobs.failing ? `${jobs.failing} failing` : 'None failing' }}</span>
                </div>
            </template>
        </OverviewHealthTile>
    </div>
</template>
