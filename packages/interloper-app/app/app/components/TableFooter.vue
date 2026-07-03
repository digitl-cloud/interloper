<script setup lang="ts">
/** Table caption + pagination bar (caption text via the default slot). */
withDefaults(defineProps<{
    page?: number
    total?: number
    pageSize?: number
}>(), { page: 1, total: 0, pageSize: 20 })

const emit = defineEmits<{ 'update:page': [page: number] }>()
</script>

<template>
    <div class="flex items-center justify-between text-[13px] text-dimmed">
        <span><slot /></span>
        <UPagination v-if="total > pageSize"
                     :page="page"
                     :total="total"
                     :items-per-page="pageSize"
                     :sibling-count="1"
                     show-edges
                     @update:page="emit('update:page', $event)" />
    </div>
</template>
