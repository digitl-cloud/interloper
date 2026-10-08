<script setup lang="ts">
/**
 * Overview KPI tile: label, headline number with inline detail, footer visual pinned to the bottom; skeletons until loaded.
 * The whole tile links to `to` through an overlay, so a `relative` link in the detail can point at a narrower view.
 */
import type { RouteLocationRaw } from 'vue-router'

defineProps<{
    label: string
    to: RouteLocationRaw
    headline?: string | number
    headlineClass?: string
    loading?: boolean
}>()
</script>

<template>
    <UCard class="relative transition-colors hover:bg-elevated/50"
           :ui="{ body: 'flex h-full min-h-32 flex-col gap-3' }">
        <NuxtLink :to="to"
                  :aria-label="label"
                  class="absolute inset-0 rounded-[inherit]" />
        <div class="text-sm text-muted">{{ label }}</div>
        <div v-if="loading"
             class="flex h-9 items-center gap-2">
            <USkeleton class="h-7 w-12" />
            <USkeleton class="h-4 w-28" />
        </div>
        <div v-else
             class="flex items-baseline gap-2">
            <span class="text-3xl font-semibold tabular-nums text-highlighted"
                  :class="headlineClass">{{ headline }}</span>
            <span class="text-sm text-muted"><slot name="detail" /></span>
        </div>
        <div class="mt-auto flex flex-col gap-1.5">
            <template v-if="loading">
                <USkeleton class="h-7.5 w-full" />
                <USkeleton class="h-4 w-2/3" />
            </template>
            <slot v-else
                  name="footer" />
        </div>
    </UCard>
</template>
