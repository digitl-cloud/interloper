<script setup lang="ts">
import type { Run } from '~/types/run'

const props = defineProps<{
    run: Run
    duration: string | null
}>()

const appConfig = useAppConfig()

/** "Job", "Source", … after what the run targets; plain "Target" once it is gone. */
const targetKindLabel = computed(() => {
    const kind = props.run.component_kind
    return kind ? kind.charAt(0).toUpperCase() + kind.slice(1) : 'Target'
})

const items = computed(() => [
    { label: targetKindLabel.value, icon: targetIcon(props.run), value: targetLabel(props.run) },
    { label: 'Partition', icon: appConfig.icons.partition, value: props.run.partition_key ?? '—' },
    { label: 'Started', icon: appConfig.icons.run, value: formatDate(props.run.started_at) || '—' },
    { label: 'Duration', icon: 'i-lucide-timer', value: props.duration ?? '—' },
])
</script>

<template>
    <div class="flex flex-wrap items-center gap-x-6 gap-y-2">
        <div v-for="item in items"
             :key="item.label"
             class="flex min-w-0 items-center gap-2">
            <UIcon :name="item.icon"
                   class="size-3.5 shrink-0 text-dimmed" />
            <span class="text-xs text-dimmed">{{ item.label }}</span>
            <UTooltip :text="item.value">
                <span class="truncate text-sm font-medium tabular-nums">{{ item.value }}</span>
            </UTooltip>
        </div>
    </div>
</template>
