<script setup lang="ts">
/** Folded cards peeking out under a collapsed container, hinting at what it holds. */
withDefaults(defineProps<{
    layers?: number
    /** Folded cards of a nested source take the nested surface; top-level ones take the card's. */
    nested?: boolean
}>(), {
    layers: 1,
    nested: false,
})
</script>

<template>
    <span v-for="i in layers"
          :key="i"
          aria-hidden="true"
          class="graph-stack-layer pointer-events-none absolute rounded-xl border"
          :class="[
              i === layers && 'graph-stack-floor',
              nested
                  ? 'h-5 border-(--graph-nested-line) bg-(--graph-nested-bg)'
                  : 'graph-layer h-6 border-(--graph-card-line) bg-(--graph-node-bg)',
          ]"
          :style="{ left: `${8 * i}px`, right: `${8 * i}px`, bottom: `${-STACK_LAYER_H * i}px`, zIndex: -i }" />
</template>
