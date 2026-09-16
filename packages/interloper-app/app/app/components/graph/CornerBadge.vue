<script setup lang="ts">
/**
 * Round badge floating off a card corner: a status glyph (warning, drift,
 * materializing) or the destination mark. One size and one protrusion for
 * every kind, so they line up and stay clear of neighbouring cards.
 */
const props = withDefaults(defineProps<{
    icon: string
    corner: 'top-left' | 'top-right' | 'bottom-right'
    tone?: 'neutral' | 'primary' | 'warning' | 'error'
    spin?: boolean
    /** Secondary count in a small sub-pill, e.g. the number of destinations. */
    count?: number
    /** Tooltip side; defaults to the corner's outward side. */
    side?: 'top' | 'bottom'
    /** Pass-through to the tooltip's `ui` slots, for rich content. */
    tooltipUi?: Record<string, string>
}>(), {
    tone: 'neutral',
    spin: false,
    count: undefined,
    side: undefined,
    tooltipUi: undefined,
})

const CORNER = {
    'top-left': '-left-2 -top-2',
    'top-right': '-right-2 -top-2',
    'bottom-right': '-right-2 -bottom-2',
} as const

const DISC = {
    neutral: 'graph-corner-badge border-[var(--graph-card-line)] bg-default',
    warning: 'border-[color-mix(in_srgb,var(--ui-warning)_50%,var(--ui-bg))] bg-[color-mix(in_srgb,var(--ui-warning)_15%,var(--ui-bg))]',
    error: 'border-[color-mix(in_srgb,var(--ui-error)_50%,var(--ui-bg))] bg-[color-mix(in_srgb,var(--ui-error)_15%,var(--ui-bg))]',
    primary: 'border-[color-mix(in_srgb,var(--ui-primary)_50%,var(--ui-bg))] bg-[color-mix(in_srgb,var(--ui-primary)_15%,var(--ui-bg))]',
} as const

const GLYPH = {
    neutral: 'text-muted',
    warning: 'text-warning',
    error: 'text-error',
    primary: 'text-primary',
} as const

const side = computed(() => props.side ?? (props.corner === 'bottom-right' ? 'bottom' : 'top'))
</script>

<template>
    <div class="absolute z-10"
         :class="CORNER[corner]">
        <UTooltip :delay-duration="0"
                  :content="{ side, sideOffset: 6 }"
                  :ui="tooltipUi">
            <div class="relative flex size-[26px] items-center justify-center rounded-full border"
                 :class="DISC[tone]">
                <UIcon :name="icon"
                       class="size-4 shrink-0"
                       :class="[GLYPH[tone], spin && 'animate-spin']" />
                <span v-if="count && count > 1"
                      class="absolute -right-[3px] -bottom-[3px] flex h-3.5 min-w-3.5 items-center justify-center rounded-full border border-[color-mix(in_srgb,var(--ui-primary)_35%,var(--ui-bg))] bg-default px-[3px] text-[9px] font-bold leading-none text-primary">
                    {{ count }}
                </span>
            </div>
            <template #content>
                <slot />
            </template>
        </UTooltip>
    </div>
</template>
