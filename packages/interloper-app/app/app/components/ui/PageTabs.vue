<script setup lang="ts">
/**
 * A page's views as a segmented group of links: a bordered track, the active
 * view a raised chip. A view without an explicit `active` is active when its
 * route is, so path views and `?tab=` views read the same way.
 */
defineProps<{ items: PageTab[] }>()
</script>

<template>
    <nav class="inline-flex max-w-full items-center gap-1 overflow-x-auto rounded-lg bg-muted p-1 ring ring-default">
        <ULink v-for="item in items"
               :key="item.label"
               :to="item.to"
               :active="item.active"
               class="inline-flex shrink-0 items-center gap-1.5 rounded-md px-3 py-1 text-sm font-medium transition-colors"
               active-class="bg-default text-highlighted shadow-xs dark:bg-inverted dark:text-inverted"
               inactive-class="text-muted hover:text-highlighted">
            {{ item.label }}
            <UBadge v-if="item.badge !== undefined"
                    :label="String(item.badge)"
                    size="sm"
                    color="neutral"
                    variant="subtle" />
        </ULink>
    </nav>
</template>
