<script setup lang="ts">
/**
 * Deprecated / alpha / beta badge for a component definition, with what the level means
 * in a tooltip. Renders nothing for a stable (or unknown) maturity, so call
 * sites pass the definition's value without checking it themselves.
 */
import type { Maturity } from '~/types/catalog'
import { showsMaturity } from '~/types/catalog'

const props = defineProps<{
    maturity?: Maturity | null
}>()

const LEVELS = {
    deprecated: {
        label: 'Deprecated',
        color: 'error',
        description: 'Being phased out: it still works, but avoid it for new setups. It will be removed in a future release.',
    },
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

const level = computed(() => showsMaturity(props.maturity) ? LEVELS[props.maturity] : null)
</script>

<template>
    <UTooltip v-if="level"
              :text="level.description">
        <UBadge :color="level.color"
                variant="soft"
                size="sm"
                class="shrink-0">
            {{ level.label }}
        </UBadge>
    </UTooltip>
</template>
