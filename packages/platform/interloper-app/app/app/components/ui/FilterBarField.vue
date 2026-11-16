<script setup lang="ts">
/**
 * A labelled filter, and the look of the pill tabs it holds. Inline (a wide
 * `FilterBar`, a `CardHeader`), the label sits beside compact pills that scroll
 * sideways when they outgrow their room. In the phone's filter sheet the label
 * heads the section and the pills become a full-width list, sized for a thumb.
 */
withDefaults(defineProps<{
    label: string
    /** False for a control that names itself inline (a select reading "All types"): the label then shows only in the sheet. */
    inlineLabel?: boolean
}>(), { inlineLabel: true })

const stacked = inject(FILTER_BAR_STACKED, ref(false))
</script>

<template>
    <UTheme :props="{ tabs: { variant: 'pill', content: false, size: stacked ? 'md' : 'sm', orientation: stacked ? 'vertical' : 'horizontal' } }">
        <div v-if="stacked"
             class="flex flex-col gap-2">
            <span class="caption text-muted">{{ label }}</span>
            <slot />
        </div>
        <div v-else
             class="flex min-w-0 max-w-full items-center gap-2">
            <span v-if="inlineLabel"
                  class="shrink-0 text-sm text-muted">{{ label }}</span>
            <slot />
        </div>
    </UTheme>
</template>
