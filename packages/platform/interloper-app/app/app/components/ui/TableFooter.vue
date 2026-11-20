<script setup lang="ts">
/**
 * A table card's #footer, shown only when it has something to say: what the
 * default slot says, "3 of 12 selected" while rows are selected, or, once the
 * rows outrun a page, where the page sits ("21–40 of 119 runs") with arrows to
 * the pages around it. A list that fits one page needs no count.
 */
const props = withDefaults(defineProps<{
    page?: number
    total?: number
    pageSize?: number
    /** What one row is, singular: "run" reads "of 119 runs". */
    noun?: string
    /** How many rows are selected. */
    selected?: number
}>(), { page: 1, total: 0, pageSize: 20, noun: 'row', selected: 0 })

const emit = defineEmits<{ 'update:page': [page: number] }>()
const slots = useSlots()
const appConfig = useAppConfig()

const paged = computed(() => props.total > props.pageSize)
const pages = computed(() => Math.max(1, Math.ceil(props.total / props.pageSize)))
const plural = computed(() => (props.total === 1 ? props.noun : `${props.noun}s`))
const caption = computed(() => {
    if (props.selected > 0) return `${props.selected} of ${props.total} selected`
    const first = (props.page - 1) * props.pageSize + 1
    const last = Math.min(props.page * props.pageSize, props.total)
    return `${first}–${last} of ${props.total} ${plural.value}`
})
</script>

<template>
    <div v-if="slots.default || selected > 0 || paged"
         class="flex items-center justify-between gap-3 text-sm text-muted">
        <span class="tabular-nums"><slot>{{ caption }}</slot></span>
        <div v-if="paged"
             class="flex items-center gap-1">
            <UButton :icon="appConfig.ui.icons.chevronLeft"
                     color="neutral"
                     variant="ghost"
                     aria-label="Previous page"
                     :disabled="page <= 1"
                     @click="emit('update:page', page - 1)" />
            <UButton :icon="appConfig.ui.icons.chevronRight"
                     color="neutral"
                     variant="ghost"
                     aria-label="Next page"
                     :disabled="page >= pages"
                     @click="emit('update:page', page + 1)" />
        </div>
    </div>
</template>
