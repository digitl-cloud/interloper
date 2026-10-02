<script setup lang="ts">
/**
 * Alpha / beta badge for a component definition, with what the level means
 * in a tooltip. Renders nothing for a stable (or unknown) maturity, so call
 * sites pass the definition's value without checking it themselves.
 */
import type { Maturity } from '~/types/catalog'

const props = defineProps<{
    maturity?: Maturity | null
}>()

const LEVELS = {
    alpha: {
        label: 'Alpha',
        color: 'warning',
        description: 'Early work, not yet proven against a live service. Behaviour, configuration and output may still change.',
    },
    beta: {
        label: 'Beta',
        color: 'info',
        description: 'Works and is being proven in use. Changes are still possible.',
    },
} as const

const level = computed(() =>
    props.maturity === 'alpha' || props.maturity === 'beta' ? LEVELS[props.maturity] : null,
)
</script>

<template>
    <UTooltip v-if="level"
              :text="level.description">
        <UBadge :color="level.color"
                variant="subtle"
                size="sm"
                class="shrink-0">
            {{ level.label }}
        </UBadge>
    </UTooltip>
</template>
