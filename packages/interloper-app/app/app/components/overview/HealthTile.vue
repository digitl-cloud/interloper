<script setup lang="ts">
/** Overview KPI tile: label, headline number with inline detail, footer visual pinned to the bottom. */
import { NuxtLink } from '#components'

defineProps<{
    label: string
    to: string
    headline: string | number
    headlineClass?: string
}>()
</script>

<template>
    <UCard :as="NuxtLink"
           :to="to"
           class="transition-colors hover:bg-elevated/50"
           :ui="{ body: 'flex h-full min-h-32 flex-col gap-3' }">
        <div class="text-sm text-muted">{{ label }}</div>
        <div class="flex items-baseline gap-2">
            <span class="text-3xl font-semibold tabular-nums text-highlighted"
                  :class="headlineClass">{{ headline }}</span>
            <span class="text-sm text-muted"><slot name="detail" /></span>
        </div>
        <div class="mt-auto flex flex-col gap-1.5">
            <slot name="footer" />
        </div>
    </UCard>
</template>
