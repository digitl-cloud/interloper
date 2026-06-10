<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, TableRow } from '@nuxt/ui'
import type { EventType } from '~/types/event'
import type { RunEvent } from '~/stores/events'
import { LazyExecutionsErrorDetailModal } from '#components'

const UBadge = resolveComponent('UBadge')
const UButton = resolveComponent('UButton')

const props = defineProps<{
    events: RunEvent[]
    loading?: boolean
    loadingMore?: boolean
    hasMore?: boolean
    /** Loads the next page; returns the in-flight promise so infinite scroll awaits it. */
    loadMore?: () => Promise<void>
}>()

const eventInFocus = defineModel<RunEvent | null>('eventInFocus', { default: null })
const assetDisplayName = useAssetDisplayName()

// UTable exposes its `<table>` element; its parent is the scrolling container
// the sticky header sticks to. We attach infinite scroll to that element so
// reaching the bottom pulls the next page (including the terminal events).
const table = useTemplateRef('table')
const scrollEl = computed<HTMLElement | null>(() => {
    const el = unref(table.value?.tableRef) as HTMLTableElement | null | undefined
    return el?.parentElement ?? null
})

useInfiniteScroll(
    scrollEl,
    () => props.loadMore?.(),
    {
        distance: 200,
        canLoadMore: () => !!props.hasMore && !props.loading && !props.loadingMore,
    },
)

const overlay = useOverlay()

function onRowHover(_e: globalThis.Event, row: TableRow<RunEvent> | null) {
    eventInFocus.value = row?.original ?? null
}

function showErrorDetail(event: RunEvent) {
    const modal = overlay.create(LazyExecutionsErrorDetailModal, {
        props: { event },
        destroyOnClose: true,
    })
    modal.open()
}

function formatTimestamp(value: string): string {
    const date = new Date(value)
    const hours = date.getHours().toString().padStart(2, '0')
    const m = date.getMinutes().toString().padStart(2, '0')
    const s = date.getSeconds().toString().padStart(2, '0')
    const ms = date.getMilliseconds().toString().padStart(3, '0')
    return `${hours}:${m}:${s}.${ms}`
}

const columns: TableColumn<RunEvent>[] = [
    {
        accessorKey: 'timestamp',
        header: 'Time',
        cell: ({ row }) => h('span', { class: 'font-mono text-xs text-muted' }, formatTimestamp(row.getValue<string>('timestamp'))),
    },
    {
        accessorKey: 'asset_key',
        header: 'Asset',
        cell: ({ row }) => {
            const event = row.original as RunEvent
            const displayName = event.asset_id ? assetDisplayName.value.get(event.asset_id) : null
            const label = displayName ?? event.asset_key
            if (!label) return h('span', { class: 'text-muted' }, '—')
            return h(UBadge, { color: 'neutral', variant: 'subtle' }, () => label)
        },
    },
    {
        accessorKey: 'event_type',
        header: 'Event',
        cell: ({ row }) => {
            const et = row.getValue<EventType>('event_type')
            return h(UBadge, {
                color: eventTypeColor(et),
                variant: 'subtle',
                icon: eventTypeIcon(et),
            }, () => eventTypeLabel(et))
        },
    },
    {
        id: 'details',
        header: 'Details',
        cell: ({ row }) => {
            const event = row.original as RunEvent
            if (event.error) {
                return h('div', { class: 'flex items-center gap-2 max-w-[400px]' }, [
                    h('span', { class: 'truncate text-sm text-error' }, event.error),
                    h(UButton, {
                        icon: 'i-lucide-expand',
                        label: 'view',
                        size: 'xs',
                        color: 'neutral',
                        variant: 'subtle',
                        class: 'shrink-0',
                        onClick: (e: MouseEvent) => {
                            e.stopPropagation()
                            showErrorDetail(event)
                        },
                    }),

                ])
            }
            const text = event.message || ''
            return h('span', { class: 'max-w-[400px] truncate text-sm text-muted' }, text)
        },
    },
]
</script>

<template>
    <UTable ref="table"
            :data="events"
            :columns="columns"
            :loading="loading"
            sticky
            :ui="{ tr: 'h-10' }"
            class="h-full"
            :on-hover="onRowHover">
        <template #body-bottom>
            <div v-if="loadingMore"
                 class="flex items-center justify-center gap-2 py-3 text-xs text-muted">
                <UIcon name="i-lucide-loader-circle"
                       class="size-4 animate-spin" />
                Loading more events…
            </div>
        </template>
    </UTable>
</template>
