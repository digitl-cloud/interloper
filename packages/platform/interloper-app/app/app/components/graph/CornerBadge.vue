<script setup lang="ts">
/**
 * Round badge floating off a card corner: a status glyph (warning, drift,
 * materializing) or the destination mark, with what it means in a tooltip
 * (the default slot). One size and one protrusion for every kind, so they
 * line up and stay clear of neighbouring cards.
 */
const props = withDefaults(defineProps<{
    icon: string
    corner: 'top-left' | 'top-right' | 'bottom-right'
    tone?: 'neutral' | 'primary' | 'warning' | 'error'
    spin?: boolean
    /** Secondary count in a small badge on the disc, e.g. the number of destinations. */
    count?: number
    /** Tooltip side; defaults to the corner's outward side. */
    side?: 'top' | 'bottom'
}>(), {
    tone: 'neutral',
    spin: false,
    count: undefined,
    side: undefined,
})

const CORNER = {
    'top-left': '-left-2 -top-2',
    'top-right': '-right-2 -top-2',
    'bottom-right': '-right-2 -bottom-2',
} as const

// The tint sits on an opaque disc so the card edge under the corner doesn't show through it.
const DISC = {
    neutral: 'border-(--graph-card-line)',
    warning: 'border-warning/50 bg-warning/10',
    error: 'border-error/50 bg-error/10',
    primary: 'border-primary/50 bg-primary/10',
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
                  :content="{ side, sideOffset: 8 }"
                  :ui="{ content: 'h-auto' }">
            <div class="relative rounded-full bg-(--graph-node-bg)"
                 :class="tone === 'neutral' && 'graph-corner-badge'">
                <div class="flex size-7 items-center justify-center rounded-full border"
                     :class="DISC[tone]">
                    <UIcon :name="icon"
                           class="size-4 shrink-0"
                           :class="[GLYPH[tone], spin && 'animate-spin']" />
                </div>
                <UBadge v-if="count && count > 1"
                        color="neutral"
                        size="xs"
                        class="absolute -right-1 -bottom-1">
                    <span class="tabular-nums">{{ count }}</span>
                </UBadge>
            </div>
            <template #content>
                <slot />
            </template>
        </UTooltip>
    </div>
</template>
