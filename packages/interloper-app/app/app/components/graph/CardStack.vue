<script setup lang="ts">
/** Folded cards peeking out under a collapsed container, hinting at what it holds. */
withDefaults(defineProps<{
    layers?: number
    /** Folded cards of a nested source take the nested surface; top-level ones are white with the card's border. */
    nested?: boolean
    borderClass?: string
}>(), {
    layers: 1,
    nested: false,
    borderClass: 'border-[var(--graph-card-line)]',
})
</script>

<template>
    <span v-for="i in layers"
          :key="i"
          aria-hidden="true"
          class="graph-stack-layer pointer-events-none absolute rounded-xl border"
          :class="[
              i === layers && 'graph-stack-floor',
              nested ? 'h-5 border-[var(--graph-nested-line)] bg-[var(--graph-nested-bg)]' : ['graph-layer h-6 bg-default', borderClass],
          ]"
          :style="{ left: `${8 * i}px`, right: `${8 * i}px`, bottom: `${-6 * i}px`, zIndex: -i }" />
</template>
