<script setup lang="ts">
import type { TableColumn } from '@nuxt/ui'
import type { AssetUIContext } from '~/composables/extensions'
import { qualifiedKey, schemaColumns } from '~/types/catalog'
import type { AssetRows } from '~/types/rows'

/**
 * An asset's rows as its destination holds them, 100 a page: the default
 * preview of an asset in the asset UI modal. The search looks for its words in
 * every text column. When the read stops short of the whole table, the footer
 * says the table holds more than it counted.
 */
const props = defineProps<{
    context: AssetUIContext
}>()

type Row = Record<string, unknown>

const PAGE_SIZE = 100

const { read } = useAssetRows()
const catalogStore = useCatalogStore()
const appConfig = useAppConfig()

const schema = computed(() => catalogStore.getAssetDefinition(qualifiedKey(props.context.source.key, props.context.asset.key))?.asset_schema)

const search = ref('')
const query = refDebounced(search, 300)
const page = ref(1)
const result = shallowRef<AssetRows | null>(null)
const loading = ref(false)
const error = ref<unknown>(null)

/** A search looks in text cells: an asset whose schema declares no text column has nothing to search. */
const searchable = computed(() => !schema.value
    || schemaColumns(schema.value).some(column => column.property.type === 'string'))

let latest = 0

async function load() {
    const request = ++latest
    loading.value = true
    error.value = null
    try {
        const rows = await read(props.context.asset.id, { search: query.value, limit: PAGE_SIZE, offset: (page.value - 1) * PAGE_SIZE })
        if (request === latest) result.value = rows
    }
    catch (e) {
        if (request === latest) error.value = e
    }
    finally {
        if (request === latest) loading.value = false
    }
}

watch(query, () => {
    page.value = 1
})
watch([() => props.context.asset.id, query, page], load, { immediate: true })

const columns = computed<TableColumn<Row>[]>(() => (result.value?.columns ?? []).map(key => ({
    id: key,
    header: key,
    accessorFn: row => row[key],
    cell: ({ row }) => rowCell(row.original[key]),
})))

/**
 * Another page follows only when this one came back full and more rows are
 * known: the total counts past it, or, unsearched, the read stopped short. A
 * searched read never reaches past its cap, so its total bounds the pages, and
 * no read reaches past the cap, where a page comes back empty.
 */
const hasNext = computed(() => {
    const rows = result.value
    if (!rows || rows.rows.length < PAGE_SIZE) return false
    return page.value * PAGE_SIZE < rows.total || (rows.truncated && !query.value)
})
/** What the footer pages over: one row past this page while another one follows. */
const pagerTotal = computed(() => Math.max(result.value?.total ?? 0, hasNext.value ? page.value * PAGE_SIZE + 1 : 0))
const firstShown = computed(() => (page.value - 1) * PAGE_SIZE + 1)
const lastShown = computed(() => (page.value - 1) * PAGE_SIZE + (result.value?.rows.length ?? 0))
</script>

<template>
    <div class="flex flex-col gap-3">
        <FilterBar v-if="searchable">
            <template #search>
                <UInput v-model="search"
                        placeholder="Search…"
                        :icon="appConfig.ui.icons.search"
                        class="w-full max-w-sm sm:w-auto" />
            </template>
        </FilterBar>

        <LoadError v-if="error"
                   title="Couldn't load the rows"
                   :error="error"
                   @retry="load" />

        <div v-if="!result && loading"
             class="flex items-center justify-center py-12">
            <UIcon :name="appConfig.ui.icons.loading"
                   class="size-5 animate-spin text-dimmed" />
        </div>

        <template v-else-if="result">
            <UTable :data="result.rows"
                    :columns="columns"
                    :loading="loading">
                <template #empty>
                    <UEmpty variant="naked"
                            size="sm"
                            :icon="appConfig.icons.rows"
                            :title="query ? 'No rows match' : 'No rows yet'" />
                </template>
            </UTable>

            <TableFooter v-model:page="page"
                         :total="pagerTotal"
                         :page-size="PAGE_SIZE">
                <template v-if="result.truncated && result.rows.length"
                          #default>
                    {{ firstShown }}–{{ lastShown }} of more than {{ result.total }} rows
                </template>
            </TableFooter>
        </template>
    </div>
</template>
