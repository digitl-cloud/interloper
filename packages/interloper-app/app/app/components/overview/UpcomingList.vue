<script setup lang="ts">
import type { UpcomingRun } from '~/types/overview'

const props = defineProps<{ items: UpcomingRun[] }>()
const LIMIT = 4

const userStore = useUserStore()
const timezone = computed(() => userStore.user?.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone)

const rows = computed(() => props.items.slice(0, LIMIT).map((item) => {
    const at = new Date(item.next_run_at)
    return {
        ...item,
        rel: relativeTime(at),
        at: `${formatShortDay(at)} ${formatClockTime(at)}`,
    }
}))
</script>

<template>
    <OverviewSection title="Coming up"
                     :meta="timezone"
                     link-label="All jobs"
                     :link-to="kindPath('job')">
        <div class="overflow-hidden rounded-lg border border-default divide-y divide-default">
            <NuxtLink v-for="row in rows"
                      :key="row.job_id"
                      :to="kindPath('job')"
                      class="flex items-center gap-3 px-4 py-[11px] text-highlighted transition-colors hover:bg-muted">
                <UIcon name="i-lucide-calendar-clock"
                       class="size-4 shrink-0 text-dimmed" />
                <span class="min-w-0 flex-1 truncate font-mono text-[12.5px]">{{ row.job_name }}</span>
                <span class="whitespace-nowrap text-[13px] font-semibold">{{ row.rel }}</span>
                <span class="w-[132px] shrink-0 whitespace-nowrap text-right text-xs tabular-nums text-dimmed">{{ row.at }}</span>
            </NuxtLink>
            <div v-if="!rows.length"
                 class="px-4 py-5 text-sm text-muted">Nothing scheduled.</div>
        </div>
    </OverviewSection>
</template>
