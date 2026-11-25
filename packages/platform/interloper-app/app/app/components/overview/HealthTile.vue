<script setup lang="ts">
/**
 * Overview KPI tile: label, headline number with one line of detail, and a footer visual pinned to the
 * bottom; skeletons until loaded. With nothing to draw (`empty`) the tile keeps its label, number and
 * detail, and no footer: the number already says it, and a bare track would read as a loader. The
 * tile keeps the room of its tallest footer (the runs sparkline, 32px) whether loading, empty or
 * drawn, so the row holds one height as the numbers land.
 * The whole tile links to `to` through an overlay, so a `relative` link in the detail can point at a narrower view.
 */
import type { RouteLocationRaw } from 'vue-router'

defineProps<{
    label: string
    to: RouteLocationRaw
    headline?: string | number
    headlineClass?: string
    loading?: boolean
    empty?: boolean
}>()
</script>

<template>
    <UCard class="relative transition-colors hover:bg-elevated/50"
           :ui="{ body: 'flex h-full min-h-36 flex-col gap-3' }">
        <NuxtLink :to="to"
                  :aria-label="label"
                  class="absolute inset-0 rounded-lg" />
        <div class="text-sm text-muted">{{ label }}</div>
        <div v-if="loading"
             class="flex h-9 items-center gap-2">
            <USkeleton class="h-7 w-12" />
            <USkeleton class="h-4 w-28" />
        </div>
        <div v-else
             class="flex animate-appear items-baseline gap-2">
            <span class="text-3xl font-semibold tabular-nums"
                  :class="headlineClass || 'text-highlighted'">{{ headline }}</span>
            <span class="text-sm tabular-nums text-muted"><slot name="detail" /></span>
        </div>
        <div class="mt-auto flex flex-col gap-1.5">
            <USkeleton v-if="loading"
                       class="h-8 w-full" />
            <div v-else-if="!empty"
                 class="flex animate-appear flex-col gap-1.5">
                <slot name="footer" />
            </div>
        </div>
    </UCard>
</template>
