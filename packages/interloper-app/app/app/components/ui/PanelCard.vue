<script setup lang="ts">
/**
 * Design "Panel": a naked header (icon + title + description) above a bordered,
 * muted card whose rows divide with the line color. The danger tone tints the
 * card red for destructive sections.
 */
withDefaults(defineProps<{
    title: string
    description?: string
    icon?: string
    iconClass?: string
    /** Small neutral badge at the right of the header (e.g. a count). */
    badge?: string | number
    /** Accent link at the right of the header. */
    linkLabel?: string
    linkTo?: string
    tone?: 'default' | 'danger'
}>(), {
    tone: 'default',
    description: undefined,
    icon: undefined,
    iconClass: undefined,
    badge: undefined,
    linkLabel: undefined,
    linkTo: undefined,
})
</script>

<template>
    <section>
        <div class="mb-3 flex items-center gap-2.5">
            <UIcon v-if="icon"
                   :name="icon"
                   class="size-4 shrink-0"
                   :class="iconClass ?? 'text-muted'" />
            <div class="min-w-0">
                <div class="text-[15px] font-semibold"
                     :class="tone === 'danger' ? 'text-error' : 'text-highlighted'">{{ title }}</div>
                <div v-if="description"
                     class="mt-1 text-[13.5px] leading-normal text-dimmed">{{ description }}</div>
            </div>
            <UBadge v-if="badge !== undefined"
                    color="neutral"
                    size="sm"
                    class="ml-auto"
                    :label="String(badge)" />
            <ULink v-else-if="linkLabel && linkTo"
                   :to="linkTo"
                   class="ml-auto whitespace-nowrap text-[13px] font-medium text-primary">{{ linkLabel }} →</ULink>
        </div>
        <div class="overflow-hidden rounded-lg border"
             :class="tone === 'danger'
                 ? 'border-error/30 bg-error/5 divide-y divide-error/20'
                 : 'border-default bg-muted divide-y divide-default'">
            <slot />
        </div>
    </section>
</template>
