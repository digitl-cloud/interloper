<script setup lang="ts">
import type { DayAggregate } from '~/composables/coverage'

const props = defineProps<{
    date: string
    day: DayAggregate | undefined
}>()

const label = computed(() => formatCalendarDay(props.date, { weekday: true }))

const counts = computed(() => (props.day ? stateCounts(props.day) : []))
</script>

<template>
    <div class="flex min-w-36 flex-col gap-1.5 py-1 text-xs">
        <span class="font-medium text-highlighted">{{ label }}</span>
        <span v-if="!day"
              class="text-muted">No partitions</span>
        <span v-for="count in counts"
              :key="count.key"
              class="flex items-center gap-1.5"
              :class="count.color">
            <UIcon :name="count.icon"
                   class="size-3.5 shrink-0" />
            <span class="flex-1 capitalize">{{ count.label }}</span>
            <span class="tabular-nums">{{ count.value.toLocaleString() }}</span>
        </span>
    </div>
</template>
