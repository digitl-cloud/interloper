<script setup lang="ts">
/** Design health tile: uppercase label, big tabular number with inline detail, footer visual pinned to the bottom. */
defineProps<{
    label: string
    to: string
    headline: string | number
    headlineClass?: string
}>()
</script>

<template>
    <NuxtLink :to="to"
              class="flex min-h-[132px] min-w-0 flex-col gap-3 rounded-lg border border-default bg-muted px-[18px] py-4 text-highlighted transition-colors hover:border-accented hover:bg-elevated">
        <div class="text-xs font-medium uppercase tracking-[.04em] text-dimmed">{{ label }}</div>
        <div class="flex items-baseline gap-2">
            <span class="text-[28px] font-semibold tracking-[-.02em] tabular-nums"
                  :class="headlineClass">{{ headline }}</span>
            <span class="text-[12.5px] text-muted"><slot name="detail" /></span>
        </div>
        <div class="mt-auto flex flex-col gap-1.5">
            <slot name="footer" />
        </div>
    </NuxtLink>
</template>
