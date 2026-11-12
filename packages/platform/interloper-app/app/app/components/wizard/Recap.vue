<script setup lang="ts">
/** What the wizard's earlier steps chose, recapped on its last step: icon, label and value rows in a card. */
defineProps<{
    rows: { icon: string, label: string, value: string }[]
}>()
</script>

<template>
    <UCard>
        <div class="-my-2 divide-y divide-default">
            <div v-for="row in rows"
                 :key="row.label"
                 class="flex items-center gap-2 py-2">
                <UIcon :name="row.icon"
                       class="size-4 shrink-0 text-dimmed" />
                <span class="shrink-0 text-sm text-muted">{{ row.label }}</span>
                <span class="min-w-0 flex-1 truncate text-right text-sm font-semibold text-highlighted">{{ row.value }}</span>
            </div>
        </div>
    </UCard>
</template>
