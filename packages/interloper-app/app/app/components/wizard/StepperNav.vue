<script setup lang="ts">
/**
 * Reusable navigation footer for stepper components.
 *
 * Renders a Back / Next|Submit button pair. Designed to be used in a
 * UDrawer's `#footer` slot, wired to a stepper's exposed navigation state.
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
</script>

<template>
    <div class="flex justify-between w-full">
        <UButton color="neutral"
                 variant="outline"
                 :label="backLabel"
                 :disabled="!hasPrev"
                 @click="emit('prev')" />
        <UButton :disabled="!canProceed || submitting"
                 :loading="submitting"
                 :label="submitLabel"
                 :trailing-icon="lastStep ? undefined : 'i-lucide-arrow-right'"
                 @click="emit('next')" />
    </div>
</template>
