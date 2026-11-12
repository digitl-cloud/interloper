<script setup lang="ts">
/**
 * Reusable navigation footer for stepper components.
 *
 * Renders Back on the left and Next or Submit on the right, the drawers' one
 * footer pattern. Designed to be used in a UDrawer's `#footer` slot, wired to
 * a stepper's exposed navigation state.
 */
withDefaults(defineProps<{
    canProceed?: boolean
    hasPrev?: boolean
    submitting?: boolean
    submitLabel?: string
    backLabel?: string
    /** On the last step the primary button drops its forward arrow. */
    lastStep?: boolean
}>(), {
    canProceed: false,
    hasPrev: false,
    submitting: false,
    submitLabel: 'Next',
    backLabel: 'Back',
    lastStep: false,
})

const emit = defineEmits<{
    next: []
    prev: []
}>()

const appConfig = useAppConfig()
</script>

<template>
    <div class="flex w-full justify-between gap-2">
        <UButton color="neutral"
                 variant="outline"
                 :label="backLabel"
                 :disabled="!hasPrev"
                 @click="emit('prev')" />
        <UButton :disabled="!canProceed || submitting"
                 :loading="submitting"
                 :label="submitLabel"
                 :trailing-icon="lastStep ? undefined : appConfig.ui.icons.arrowRight"
                 @click="emit('next')" />
    </div>
</template>
