<script setup lang="ts">
const props = withDefaults(defineProps<{
    title?: string
    description?: string
    confirmLabel?: string
    cancelLabel?: string
    confirmColor?: 'error' | 'primary' | 'neutral'
    icon?: string
}>(), {
    title: 'Are you sure?',
    description: 'This action cannot be undone.',
    confirmLabel: 'Delete',
    cancelLabel: 'Cancel',
    confirmColor: 'error',
    icon: 'i-lucide-triangle-alert',
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
                <p class="text-sm text-muted">
                    {{ props.description }}
                </p>
            </div>
        </template>

        <template #footer>
            <UButton :label="props.cancelLabel"
                     color="neutral"
                     variant="outline"
                     @click="emit('close', false)" />
            <UButton :label="props.confirmLabel"
                     :color="props.confirmColor"
                     @click="emit('close', true)" />
        </template>
    </UModal>
</template>
