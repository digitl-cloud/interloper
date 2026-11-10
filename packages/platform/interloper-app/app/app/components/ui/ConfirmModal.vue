<script setup lang="ts">
import type { UsedByRef } from '~/utils/apiErrors'

const props = withDefaults(defineProps<{
    title?: string
    /** Body text. A ``{subject}`` token is replaced by a badge of ``subject``. */
    description?: string
    confirmLabel?: string
    cancelLabel?: string
    confirmColor?: 'error' | 'primary' | 'neutral'
    icon?: string
    /** The entity the action targets, rendered as an inline badge at ``{subject}``. */
    subject?: { name: string, icon?: string }
    /** Referrers that block the action — listed, and the confirm button disabled. */
    blocking?: UsedByRef[]
    /** Referrers the target will be detached from — listed as a heads-up. */
    detaching?: UsedByRef[]
}>(), {
    title: 'Are you sure?',
    description: 'This action cannot be undone.',
    confirmLabel: 'Delete',
    cancelLabel: 'Cancel',
    confirmColor: 'error',
    icon: 'i-lucide-triangle-alert',
    subject: undefined,
    blocking: () => [],
    detaching: () => [],
})

const emit = defineEmits<{
    close: [confirmed: boolean]
}>()

// Text segments around the {subject} token; the badge slots between them.
// Without a subject (or token), this is just the whole description, no badge.
const descriptionParts = computed(() =>
    props.subject ? props.description.split('{subject}') : [props.description],
)
</script>

<template>
    <UModal :title="props.title"
            :close="{ onClick: () => emit('close', false) }"
            :ui="{ footer: 'justify-end' }">
        <template #body>
            <div class="flex gap-4 items-start">
                <div class="flex size-10 shrink-0 items-center justify-center rounded-full bg-error/10">
                    <UIcon :name="props.icon"
                           class="size-5 text-error" />
                </div>
                <div class="flex-1 space-y-3 text-sm text-muted">
                    <template v-if="props.blocking.length">
                        <p>Still in use — rebind or delete these first:</p>
                        <UsedByTable :referrers="props.blocking" />
                    </template>
                    <template v-else>
                        <p>
                            <template v-for="(part, i) in descriptionParts"
                                      :key="i">{{ part }}<EntityBadge v-if="props.subject && i < descriptionParts.length - 1"
                                            :label="props.subject.name"
                                            :icon="props.subject.icon"
                                            class="mx-0.5 align-middle" /></template>
                        </p>
                        <template v-if="props.detaching.length">
                            <p>It will also be removed from:</p>
                            <UsedByTable :referrers="props.detaching" />
                        </template>
                    </template>
                </div>
            </div>
        </template>

        <template #footer>
            <UButton :label="props.cancelLabel"
                     color="neutral"
                     variant="outline"
                     @click="emit('close', false)" />
            <UButton :label="props.confirmLabel"
                     :color="props.confirmColor"
                     :disabled="props.blocking.length > 0"
                     @click="emit('close', true)" />
        </template>
    </UModal>
</template>
