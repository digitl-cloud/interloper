<script setup lang="ts">
/**
 * A table card's #footer: the row count, or what the default slot says
 * instead, and the pager once the rows outrun a page. The count reads "12
 * runs" (from `noun`), or "3 of 12 selected" while rows are selected.
 */
const props = withDefaults(defineProps<{
    page?: number
    total?: number
    pageSize?: number
    /** What one row is, singular: "run" reads "12 runs". */
    noun?: string
    /** How many rows are selected. */
    selected?: number
}>(), { page: 1, total: 0, pageSize: 20, noun: 'row', selected: 0 })

const emit = defineEmits<{ 'update:page': [page: number] }>()

const caption = computed(() => props.selected > 0
    ? `${props.selected} of ${props.total} selected`
    : `${props.total} ${props.total === 1 ? props.noun : `${props.noun}s`}`)
</script>

<template>
    <div class="flex items-center justify-between gap-3 text-sm text-muted">
        <span class="tabular-nums"><slot>{{ caption }}</slot></span>
        <UPagination v-if="total > pageSize"
                     :page="page"
                     :total="total"
                     :items-per-page="pageSize"
                     :sibling-count="1"
                     show-edges
                     @update:page="emit('update:page', $event)" />
    </div>
</template>
