<script setup lang="ts">
/**
 * Shared chrome for the create/edit wizards: right drawer at the design
 * width, close button, and the StepperNav footer wired to the stepper
 * exposed by the body slot (pages keep `ref="stepperRef"` on their stepper
 * and pass it back via the `stepper` prop).
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
             :title="stepper?.title ?? defaultTitle"
             :ui="{ content: 'w-[35rem]', description: 'sr-only' }">
        <template #description>
            {{ description }}
        </template>
        <template #body>
            <UButton icon="i-lucide-x"
                     color="neutral"
                     variant="soft"
                     size="md"
                     class="absolute top-[22px] right-[26px] rounded-[9px] text-muted"
                     aria-label="Close"
                     @click="open = false" />
            <slot />
        </template>
        <template #footer>
            <StepperNav v-if="stepper"
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
