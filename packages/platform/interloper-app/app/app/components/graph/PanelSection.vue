<script setup lang="ts">
/**
 * A collapsible section card of the asset panel, open by default, with an
 * optional count beside its title. The `title` slot replaces the plain title;
 * the `actions` slot sits at the header's end, beside the fold toggle rather
 * than inside it.
 */
defineProps<{
    title?: string
    count?: number
}>()

const appConfig = useAppConfig()
const open = ref(true)
</script>

<template>
    <div class="rounded-lg bg-muted">
        <div class="flex items-center">
            <button class="group flex min-w-0 flex-1 cursor-pointer items-center gap-2 p-4"
                    :class="$slots.actions && 'pe-2 sm:pe-2'"
                    :data-state="open ? 'open' : 'closed'"
                    :aria-expanded="open"
                    @click="open = !open">
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
            <div v-if="$slots.actions"
                 class="flex shrink-0 items-center pe-3 sm:pe-5">
                <slot name="actions" />
            </div>
        </div>

        <UCollapsible v-model:open="open">
            <template #content>
                <div class="px-4 pb-4">
                    <slot />
                </div>
            </template>
        </UCollapsible>
    </div>
</template>
