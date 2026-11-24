<script setup lang="ts">
/**
 * A filter, and the look of the pill tabs it holds. Inline (a wide
 * `FilterBar`, a `CardHeader`), the control names itself: its label is only
 * the group's name for screen readers, and its pills stand as tall as the
 * controls beside them, a toolbar's inputs in a `FilterBar`, a card header's
 * small buttons elsewhere, scrolling sideways when they outgrow their room.
 * In the phone's filter sheet the label heads the section and the pills
 * become a full-width list, sized for a thumb.
 */
defineProps<{
    label: string
}>()

// Provided only inside a FilterBar; a field without it stands in a card header.
const bar = inject(FILTER_BAR_STACKED, null)
const stacked = computed(() => bar?.value ?? false)
const size = computed(() => stacked.value ? 'md' : bar ? 'sm' : 'xs')
</script>

<template>
    <UTheme :props="{ tabs: { variant: 'pill', content: false, size, orientation: stacked ? 'vertical' : 'horizontal' } }">
        <div v-if="stacked"
             class="flex flex-col gap-2">
            <span class="caption text-muted">{{ label }}</span>
            <slot />
        </div>
        <div v-else
             role="group"
             :aria-label="label"
             class="flex min-w-0 max-w-full items-center">
            <slot />
        </div>
    </UTheme>
</template>
