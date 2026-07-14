<script setup lang="ts">
/** Design recap table: bordered card of icon | label | value rows. */
defineProps<{
    rows: { icon: string, label: string, value: string }[]
}>()
</script>

<template>
    <div class="border border-default rounded-[12px] px-3.5 py-1 bg-default">
        <div v-for="(row, i) in rows"
             :key="row.label"
             class="flex items-center gap-2.5 py-2.5"
             :class="i < rows.length - 1 ? 'border-b border-default' : ''">
            <UIcon :name="row.icon"
                   class="size-4 text-dimmed shrink-0" />
            <span class="flex-1 text-[13px] text-muted">{{ row.label }}</span>
            <span class="text-[13px] font-semibold text-highlighted text-right max-w-[60%] truncate">{{ row.value }}</span>
        </div>
    </div>
</template>
