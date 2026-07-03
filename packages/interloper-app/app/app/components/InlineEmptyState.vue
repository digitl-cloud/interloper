<script setup lang="ts">
/** Wizard placeholder when no resource exists yet: dashed card + create action. */
defineProps<{
    icon: string
    message: string
    actionLabel: string
}>()

const emit = defineEmits<{ action: [] }>()
</script>

<template>
    <div class="flex flex-col items-center gap-3 rounded-[14px] border border-dashed border-accented bg-(--ui-bg-band) px-6 py-8">
        <div class="size-10 rounded-[11px] border border-default bg-default flex items-center justify-center">
            <UIcon :name="icon"
                   class="size-6" />
        </div>
        <p class="text-sm text-muted">{{ message }}</p>
        <UButton icon="i-lucide-plus"
                 :label="actionLabel"
                 size="md"
                 @click="emit('action')" />
    </div>
</template>
