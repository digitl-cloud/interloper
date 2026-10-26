<script setup lang="ts">
/**
 * A collapsible section card of the asset panel, open by default, with an
 * optional count beside its title. The `title` slot replaces the plain title.
 */
defineProps<{
    title?: string
    count?: number
}>()
</script>

<template>
    <UCollapsible default-open
                  class="rounded-lg bg-muted ring ring-default">
        <button class="group flex w-full cursor-pointer items-center gap-2 p-4 sm:px-6">
            <slot name="title">
                <span class="font-semibold text-highlighted">{{ title }}</span>
            </slot>
            <span v-if="count"
                  class="ms-auto text-xs text-muted tabular-nums">{{ count }}</span>
            <UIcon name="i-lucide-chevron-down"
                   class="size-3.5 shrink-0 text-dimmed transition-transform duration-200 group-data-[state=closed]:-rotate-90"
                   :class="!count && 'ms-auto'" />
        </button>

        <template #content>
            <div class="px-4 pb-4 sm:px-6 sm:pb-6">
                <slot />
            </div>
        </template>
    </UCollapsible>
</template>
