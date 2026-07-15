<script setup lang="ts">
import type { UsedByRef } from '~/utils/apiErrors'

const props = withDefaults(defineProps<{
    title?: string
    description?: string
    confirmLabel?: string
    cancelLabel?: string
    confirmColor?: 'error' | 'primary' | 'neutral'
    icon?: string
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
    blocking: () => [],
    detaching: () => [],
})

const emit = defineEmits<{
    close: [confirmed: boolean]
}>()
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
                        <p>{{ props.description }}</p>
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
