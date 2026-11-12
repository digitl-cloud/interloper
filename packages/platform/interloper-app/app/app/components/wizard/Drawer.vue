<script setup lang="ts">
/**
 * Shared chrome for the create/edit wizards: a right drawer at the theme's
 * width with its close button, and the WizardStepperNav footer wired to the
 * stepper exposed by the body slot (pages keep `ref="stepperRef"` on their
 * stepper and pass it back via the `stepper` prop).
 */
interface StepperHandle {
    title: string
    canProceed: boolean
    hasPrev: boolean
    isLastStep: boolean
    submitting: boolean
    submitLabel: string
    next: () => void
    prev: () => void
}

const open = defineModel<boolean>('open', { required: true })

withDefaults(defineProps<{
    /** Fallback title while the stepper ref isn't mounted yet. */
    defaultTitle: string
    /** Accessible description (visually hidden). */
    description: string
    /** The stepper component instance exposed from the body slot. */
    stepper?: StepperHandle | null
}>(), { stepper: null })
</script>

<template>
    <UDrawer v-model:open="open"
             direction="right"
             :handle="false"
             :handle-only="true"
             close
             :title="stepper?.title ?? defaultTitle"
             :ui="{ description: 'sr-only' }">
        <template #description>
            {{ description }}
        </template>
        <template #body>
            <slot />
        </template>
        <template #footer>
            <WizardStepperNav v-if="stepper"
                        :last-step="stepper.isLastStep"
                        :can-proceed="stepper.canProceed"
                        :has-prev="stepper.hasPrev"
                        :submitting="stepper.submitting"
                        :submit-label="stepper.submitLabel"
                        @next="stepper.next()"
                        @prev="stepper.prev()" />
        </template>
    </UDrawer>
</template>
