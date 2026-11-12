<script setup lang="ts">
import type { PartitionState } from '~/composables/coverage'

const props = defineProps<{
    counts: Record<PartitionState, number>
}>()

const items = computed(() => stateCounts(props.counts))
</script>

<template>
    <span class="flex h-5 items-center gap-1.5 tabular-nums">
        <UTooltip v-for="count in items"
                  :key="count.key"
                  :text="count.text"
                  :delay-duration="0">
            <UBadge :icon="count.icon"
                    :label="count.value.toLocaleString()"
                    :color="count.badge"
                    size="sm" />
        </UTooltip>
    </span>
</template>
