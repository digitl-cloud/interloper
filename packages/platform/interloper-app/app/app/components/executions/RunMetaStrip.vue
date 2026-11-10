<script setup lang="ts">
import type { Run } from '~/types/run'

const props = defineProps<{
    run: Run
    duration: string | null
}>()

/** "Job", "Source", … after what the run targets; plain "Target" once it is gone. */
const targetKindLabel = computed(() => {
    const kind = props.run.component_kind
    return kind ? kind.charAt(0).toUpperCase() + kind.slice(1) : 'Target'
})

function clockTime(value: string | null): string {
    if (!value) return '—'
    return new Date(value).toLocaleTimeString('en-US', { hour12: false })
}

const items = computed(() => [
    { label: targetKindLabel.value, icon: 'i-lucide-calendar-clock', value: targetLabel(props.run) },
    { label: 'Partition', icon: 'i-lucide-calendar', value: props.run.partition_key ?? '—' },
    { label: 'Started', icon: 'i-lucide-play', value: clockTime(props.run.started_at) },
    { label: 'Duration', icon: 'i-lucide-timer', value: props.duration ?? '—' },
])
</script>

<template>
    <div class="flex flex-wrap items-center gap-x-7 gap-y-2">
        <div v-for="item in items"
             :key="item.label"
             class="flex min-w-0 items-center gap-2">
            <UIcon :name="item.icon"
                   class="size-3.5 shrink-0 text-dimmed" />
            <span class="text-xs text-dimmed">{{ item.label }}</span>
            <span class="truncate text-sm font-medium"
                  :title="item.value">{{ item.value }}</span>
        </div>
    </div>
</template>
