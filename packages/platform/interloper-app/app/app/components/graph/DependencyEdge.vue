<script setup lang="ts">
import { BezierEdge, getBezierPath } from '@vue-flow/core'
import type { EdgeProps } from '@vue-flow/core'

const props = defineProps<EdgeProps>()

const pathD = computed(() => {
    const [d] = getBezierPath({
        sourceX: props.sourceX,
        sourceY: props.sourceY,
        sourcePosition: props.sourcePosition,
        targetX: props.targetX,
        targetY: props.targetY,
        targetPosition: props.targetPosition,
    })
    return d
})
</script>

<template>
    <BezierEdge v-bind="props" />
    <path :d="pathD"
          fill="none"
          stroke="transparent"
          stroke-width="20"
          style="pointer-events: stroke" />
</template>
