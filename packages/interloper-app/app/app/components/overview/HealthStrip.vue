<script setup lang="ts">
import type { Overview } from '~/types/overview'

const props = defineProps<{ overview: Overview | null }>()

const SPARK_HEIGHT = 30

const spark = computed(() => {
    const hourly = props.overview?.runs.hourly ?? []
    const max = Math.max(1, ...hourly.map(b => b.succeeded + b.failed))
    return hourly.map((bucket, i) => ({
        key: bucket.hour,
        title: `${23 - i}h ago · ${bucket.succeeded} ok · ${bucket.failed} failed`,
        ok: bucket.succeeded ? Math.max(2, Math.round(SPARK_HEIGHT * bucket.succeeded / max)) : 1,
        fail: bucket.failed ? Math.max(2, Math.round(SPARK_HEIGHT * bucket.failed / max)) : 0,
    }))
})

const activity = computed(() => {
    const { running, queued, longest_running_seconds } = props.overview?.activity
        ?? { running: 0, queued: 0, longest_running_seconds: null }
    const total = running + queued
    return {
        running,
        queued,
        runningPct: total ? (100 * running) / total : 0,
        queuedPct: total ? (100 * queued) / total : 0,
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
        failing,
        healthy: enabled - failing,
        failingPct: enabled ? (100 * failing) / enabled : 0,
        healthyPct: enabled ? (100 * (enabled - failing)) / enabled : 100,
    }
})
</script>

<template>
    <div class="grid grid-cols-2 gap-4 sm:gap-6 xl:grid-cols-4">
        <OverviewHealthTile label="Runs · last 24h"
                            to="/executions/runs"
                            :loading="!overview"
                            :headline="overview?.runs.total">
            <template #detail>
                {{ overview?.runs.succeeded }} succeeded ·
                <ULink :to="{ path: '/executions/runs', query: { status: 'failed' } }"
                       class="relative hover:underline"
                       :class="overview?.runs.failed ? 'font-medium text-error' : 'text-muted'">{{ overview?.runs.failed }} failed</ULink>
            </template>
            <template #footer>
                <div class="flex items-end gap-0.5"
                     :style="{ height: `${SPARK_HEIGHT}px` }">
                    <div v-for="bar in spark"
                         :key="bar.key"
                         class="flex h-full flex-1 flex-col justify-end gap-px"
                         :title="bar.title">
                        <div v-if="bar.fail"
                             class="rounded-t-sm bg-error"
                             :style="{ height: `${bar.fail}px` }" />
                        <div class="rounded-sm"
                             :class="bar.ok > 1 ? 'bg-success' : 'bg-accented'"
                             :style="{ height: `${bar.ok}px` }" />
                    </div>
                </div>
                <div class="flex justify-between text-xs text-dimmed"><span>24h ago</span><span>now</span></div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Running now"
                            :to="{ path: '/executions/runs', query: { status: 'running' } }"
                            :loading="!overview"
                            :headline="activity.running">
            <template #detail>
                <ULink :to="{ path: '/executions/runs', query: { status: 'queued' } }"
                       class="relative text-muted hover:underline">{{ activity.queued }} queued</ULink>
            </template>
            <template #footer>
                <div class="flex h-1.5 gap-0.5 overflow-hidden rounded-full bg-accented">
                    <div class="bg-primary"
                         :style="{ width: `${activity.runningPct}%` }" />
                    <div class="bg-(--ui-text-dimmed)/40"
                         :style="{ width: `${activity.queuedPct}%` }" />
                </div>
                <div class="flex justify-between text-xs text-dimmed">
                    <span>{{ activity.running }} running · {{ activity.queued }} queued</span>
                    <span v-if="activity.longest">longest {{ activity.longest }}</span>
                </div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Backfills in progress"
                            :to="{ path: '/executions/backfills', query: { status: 'running', single: 'true' } }"
                            :loading="!overview"
                            :headline="overview?.backfills.active">
            <template #detail>{{ overview?.backfills.partitions_done }} of {{ overview?.backfills.partitions_total }} partitions</template>
            <template #footer>
                <div class="flex h-1.5 overflow-hidden rounded-full bg-accented">
                    <div class="bg-primary"
                         :style="{ width: `${backfillPct}%` }" />
                </div>
                <div class="flex justify-between text-xs text-dimmed">
                    <span>combined progress</span>
                    <span class="font-semibold text-primary">{{ backfillPct }}%</span>
                </div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Jobs failing"
                            :to="kindPath('job')"
                            :loading="!overview"
                            :headline="jobs.failing"
                            :headline-class="jobs.failing ? 'text-error' : ''">
            <template #detail>of {{ overview?.jobs.enabled }} enabled</template>
            <template #footer>
                <div class="flex h-1.5 gap-0.5 overflow-hidden rounded-full bg-accented">
                    <div class="bg-success"
                         :style="{ width: `${jobs.healthyPct}%` }" />
                    <div v-if="jobs.failing"
                         class="bg-error"
                         :style="{ width: `${jobs.failingPct}%` }" />
                </div>
                <div class="flex justify-between text-xs text-dimmed">
                    <span>{{ jobs.healthy }} healthy</span>
                    <span>{{ jobs.failing ? `${jobs.failing} failing` : 'none failing' }}</span>
                </div>
            </template>
        </OverviewHealthTile>
    </div>
</template>
