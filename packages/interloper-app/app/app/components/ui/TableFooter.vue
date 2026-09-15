<script setup lang="ts">
/**
 * Table caption + pagination bar (caption text via the default slot), ruled
 * off from the rows above. The right padding keeps the pager clear of the
 * layout's floating launcher, which publishes its width as --launcher-inset.
 */
withDefaults(defineProps<{
    page?: number
    total?: number
    pageSize?: number
}>(), { page: 1, total: 0, pageSize: 20 })

const emit = defineEmits<{ 'update:page': [page: number] }>()
</script>

<template>
    <div class="flex items-center justify-between border-t border-default pt-4 pr-[var(--launcher-inset,0px)] text-[13px] text-dimmed">
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
