<script setup lang="ts">
/**
 * A collapsible section card of the asset panel, open by default, with an
 * optional count beside its title. The `title` slot replaces the plain title.
 */
defineProps<{
    title?: string
    count?: number
}>()

const appConfig = useAppConfig()
</script>

<template>
    <UCollapsible default-open
                  class="rounded-lg bg-muted ring ring-default">
        <button class="group flex w-full cursor-pointer items-center gap-2 p-4 sm:px-6">
            <slot name="title">
                <span class="font-semibold text-highlighted">{{ title }}</span>
            </slot>
            <UBadge v-if="count"
                    color="neutral"
                    size="sm"
                    class="shrink-0">
                <span class="tabular-nums">{{ count }}</span>
            </UBadge>
            <UIcon :name="appConfig.ui.icons.chevronDown"
                   class="ms-auto size-4 shrink-0 text-dimmed transition-transform duration-200 group-data-[state=open]:rotate-180" />
        </button>

        <template #content>
            <div class="px-4 pb-4 sm:px-6 sm:pb-6">
                <slot />
            </div>
        </template>
    </UCollapsible>
</template>
