<script setup lang="ts">
/** A moment as "2d ago" or "in 14h", the exact date and time on hover. */
const props = defineProps<{
    value: string | number | Date
}>()

const date = computed(() => new Date(props.value))
</script>

<template>
    <UTooltip :text="formatDayTime(date)">
        <time :datetime="date.toISOString()"
              class="tabular-nums">{{ relativeTime(date) }}</time>
    </UTooltip>
</template>
