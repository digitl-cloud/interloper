<script setup lang="ts">
import type { Overview } from '~/types/overview'

const props = defineProps<{ overview: Overview }>()

const SPARK_HEIGHT = 30

const spark = computed(() => {
    const max = Math.max(1, ...props.overview.runs.hourly.map(b => b.succeeded + b.failed))
    return props.overview.runs.hourly.map((bucket, i) => ({
        key: bucket.hour,
        title: `${23 - i}h ago · ${bucket.succeeded} ok · ${bucket.failed} failed`,
        ok: bucket.succeeded ? Math.max(2, Math.round(SPARK_HEIGHT * bucket.succeeded / max)) : 1,
        fail: bucket.failed ? Math.max(2, Math.round(SPARK_HEIGHT * bucket.failed / max)) : 0,
    }))
})

const activity = computed(() => {
    const { running, queued, longest_running_seconds } = props.overview.activity
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
    const { partitions_done, partitions_total } = props.overview.backfills
    return partitions_total ? Math.round((100 * partitions_done) / partitions_total) : 0
})

const jobs = computed(() => {
    const { enabled, failing } = props.overview.jobs
    return {
        failing,
        healthy: enabled - failing,
        failingPct: enabled ? (100 * failing) / enabled : 0,
        healthyPct: enabled ? (100 * (enabled - failing)) / enabled : 100,
    }
})
</script>

<template>
    <div class="grid grid-cols-2 gap-4 xl:grid-cols-4">
        <OverviewHealthTile label="Runs · last 24h"
                            to="/executions/runs"
                            :headline="overview.runs.total">
            <template #detail>
                {{ overview.runs.succeeded }} succeeded ·
                <span :class="overview.runs.failed ? 'font-medium text-error' : ''">{{ overview.runs.failed }} failed</span>
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
                <div class="flex justify-between text-[10.5px] text-dimmed"><span>24h ago</span><span>now</span></div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Running now"
                            to="/executions/runs"
                            :headline="activity.running">
            <template #detail>{{ activity.queued }} queued</template>
            <template #footer>
                <div class="flex h-1.5 gap-0.5 overflow-hidden rounded-full bg-accented">
                    <div class="bg-primary"
                         :style="{ width: `${activity.runningPct}%` }" />
                    <div class="bg-(--ui-text-dimmed)/40"
                         :style="{ width: `${activity.queuedPct}%` }" />
                </div>
                <div class="flex justify-between text-[10.5px] text-dimmed">
                    <span>{{ activity.running }} running · {{ activity.queued }} queued</span>
                    <span v-if="activity.longest">longest {{ activity.longest }}</span>
                </div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Backfills in progress"
                            to="/executions/backfills"
                            :headline="overview.backfills.active">
            <template #detail>{{ overview.backfills.partitions_done }} of {{ overview.backfills.partitions_total }} partitions</template>
            <template #footer>
                <div class="flex h-1.5 overflow-hidden rounded-full bg-accented">
                    <div class="bg-primary"
                         :style="{ width: `${backfillPct}%` }" />
                </div>
                <div class="flex justify-between text-[10.5px] text-dimmed">
                    <span>combined progress</span>
                    <span class="font-semibold text-primary">{{ backfillPct }}%</span>
                </div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Jobs failing"
                            :to="kindPath('job')"
                            :headline="jobs.failing"
                            :headline-class="jobs.failing ? 'text-error' : ''">
            <template #detail>of {{ overview.jobs.enabled }} enabled</template>
            <template #footer>
                <div class="flex h-1.5 gap-0.5 overflow-hidden rounded-full bg-accented">
                    <div v-if="jobs.failing"
                         class="bg-error"
                         :style="{ width: `${jobs.failingPct}%` }" />
                    <div class="bg-success"
                         :style="{ width: `${jobs.healthyPct}%` }" />
                </div>
                <div class="flex justify-between text-[10.5px] text-dimmed">
                    <span>{{ jobs.failing ? `${jobs.failing} failing` : 'none failing' }}</span>
                    <span>{{ jobs.healthy }} healthy</span>
                </div>
            </template>
        </OverviewHealthTile>
    </div>
</template>
