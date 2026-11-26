<script setup lang="ts">
import type { TableColumn } from '@nuxt/ui'
import type { AssetUIContext } from '~/composables/extensions'
import type { SchemaColumn } from '~/types/catalog'
import { qualifiedKey, schemaColumns } from '~/types/catalog'

/**
 * The grid editor, the default UI of an editable reference:
 * one column per field of its schema, rows edited in place, added and
 * deleted, then written whole with the version they were read at, and the
 * table's source run so what reads the table catches up: at once, or, when
 * an asset of the source is partitioned, through the source's run dialog,
 * which asks for the partition as the source's own Run does. A table someone
 * changed meanwhile is never
 * overwritten: the editor says so and reloads it. A list or an object column
 * shows as JSON and keeps its values. A viewer sees the rows read-only, and so
 * does anyone when the table holds more rows than the rows API reads.
 */
const props = defineProps<{
    context: AssetUIContext
}>()

type Row = Record<string, unknown>

const PAGE_SIZE = 100
/** The most rows the rows API reads: the editor reads the whole table in one read. */
const READ_LIMIT = 10_000
/** Column types a cell cannot edit: they show as JSON, their values kept as read. */
const STRUCTURED = new Set(['array', 'object'])

const { read, write, run } = useAssetRows()
const catalogStore = useCatalogStore()
const appConfig = useAppConfig()
const toast = useToast()

const fields = computed(() => schemaColumns(
    catalogStore.getAssetDefinition(qualifiedKey(props.context.source.key, props.context.asset.key))?.asset_schema,
))
const editableFields = computed(() => fields.value.filter(field => !STRUCTURED.has(String(field.property.type))))
const columns = computed<TableColumn<Row>[]>(() => [
    ...fields.value.map(field => STRUCTURED.has(String(field.property.type))
        ? { id: field.key, header: field.key, cell: ({ row }: { row: { original: Row } }) => rowCell(row.original[field.key]) }
        : { id: field.key, header: field.key }),
    { id: 'actions', header: '' },
])
const sourceName = computed(() => {
    const { source } = props.context
    return source.name ?? catalogStore.getSourceDefinition(source.key)?.name ?? source.key
})
const partitioned = computed(() =>
    catalogStore.getSourceDefinition(props.context.source.key)?.assets.some(asset => asset.partitioning) ?? false)
const runDialogOpen = ref(false)

const rows = ref<Row[]>([])
const version = ref('')
const loaded = ref(false)
const loading = ref(false)
const loadError = ref<unknown>(null)
const conflict = ref(false)
/** A read stopped short of the whole table: saving it would drop the rows it did not read. */
const tooLarge = ref(false)
const dirty = ref(false)
const saving = ref(false)

const page = ref(1)
const pageRows = computed(() => rows.value.slice((page.value - 1) * PAGE_SIZE, page.value * PAGE_SIZE))
const lastPage = computed(() => Math.max(1, Math.ceil(rows.value.length / PAGE_SIZE)))
watch(lastPage, (last) => {
    if (page.value > last) page.value = last
})

/** Every row of the table, in one read, and the version a save names; a table past the cap is too large to edit. */
async function load() {
    loading.value = true
    loadError.value = null
    tooLarge.value = false
    try {
        const table = await read(props.context.asset.id, { limit: READ_LIMIT })
        if (table.truncated) {
            tooLarge.value = true
            return
        }
        rows.value = table.rows
        version.value = table.version ?? ''
        loaded.value = true
        dirty.value = false
        conflict.value = false
        page.value = 1
    }
    catch (e) {
        loadError.value = e
    }
    finally {
        loading.value = false
    }
}

watch(() => [props.context.asset.id, props.context.canEdit] as const, ([, canEdit]) => {
    if (canEdit) load()
}, { immediate: true })

const reloadActions = [{ label: 'Reload', icon: appConfig.ui.icons.reload, color: 'neutral' as const, variant: 'outline' as const, onClick: load }]

// -- Editing: a row is addressed by its index on the shown page ----------------

function update(index: number, field: SchemaColumn, value: unknown) {
    rows.value[(page.value - 1) * PAGE_SIZE + index]![field.key] = value
    dirty.value = true
}

function addRow() {
    const row = Object.fromEntries(fields.value.map(field => [field.key, field.property.default ?? (field.widget === 'switch' ? false : null)]))
    rows.value = [row, ...rows.value]
    page.value = 1
    dirty.value = true
}

function deleteRow(index: number) {
    rows.value.splice((page.value - 1) * PAGE_SIZE + index, 1)
    dirty.value = true
}

/** A text input's value; an optional field emptied holds null. */
function textValue(field: SchemaColumn, value: unknown): string | null {
    return value === '' && field.property.nullable ? null : String(value ?? '')
}

function numberValue(value: unknown): number | null {
    return value === '' || value === null || value === undefined ? null : Number(value)
}

/** An enum's values hold JSON scalars: the select offers them as they are. */
function enumItems(field: SchemaColumn): (string | number)[] {
    return (field.property.enum ?? []) as (string | number)[]
}

function enumValue(value: unknown): string | number | undefined {
    return typeof value === 'string' || typeof value === 'number' ? value : undefined
}

// -- Saving --------------------------------------------------------------------

async function save() {
    saving.value = true
    try {
        version.value = (await write(props.context.asset.id, rows.value, version.value)).version ?? ''
        dirty.value = false
    }
    catch (e) {
        const { status, statusCode } = e as { status?: number, statusCode?: number }
        if ((status ?? statusCode) !== 409) toast.add(errorToast(e, 'Couldn\'t save the table'))
        // A 409 is a table that changed since it was read, or one that has outgrown what the API reads.
        else if ((await read(props.context.asset.id, { limit: 1 }).catch(() => null))?.truncated) tooLarge.value = true
        else conflict.value = true
        return
    }
    finally {
        saving.value = false
    }
    if (partitioned.value) {
        toast.add({ title: 'Table saved', color: 'success' })
        runDialogOpen.value = true
        return
    }
    try {
        const runId = await run(props.context.source.id)
        toast.add({ title: 'Table saved', description: `${sourceName.value} run queued (${shortId(runId)})`, color: 'success' })
    }
    catch (e) {
        toast.add(errorToast(e, `Table saved, but couldn't run ${sourceName.value}`))
    }
}
</script>

<template>
    <div v-if="!context.canEdit || tooLarge"
         class="flex flex-col gap-3">
        <UAlert v-if="tooLarge"
                color="warning"
                :icon="appConfig.ui.icons.warning"
                title="This table holds more than 10,000 rows and cannot be edited in the app" />
        <AssetsRowsTable :context="context" />
    </div>

    <div v-else
         class="flex flex-col gap-3">
        <FilterBar>
            <template #actions>
                <UButton :icon="appConfig.ui.icons.plus"
                         label="Add row"
                         color="neutral"
                         variant="outline"
                         :disabled="!loaded"
                         @click="addRow" />
                <UTooltip :text="partitioned ? `Saves the table, then asks which partition of ${sourceName} to run` : `Saves the table, then runs ${sourceName}`">
                    <UButton :icon="appConfig.icons.run"
                             label="Save and run"
                             :loading="saving"
                             :disabled="!dirty || conflict"
                             @click="save" />
                </UTooltip>
            </template>
        </FilterBar>

        <UAlert v-if="conflict"
                color="warning"
                :icon="appConfig.ui.icons.warning"
                title="Someone changed this table since you opened it"
                description="Reload it to see their changes; yours are discarded."
                :actions="reloadActions" />
        <LoadError v-if="loadError"
                   title="Couldn't load the table"
                   :error="loadError"
                   @retry="load" />

        <div v-if="!loaded && loading"
             class="flex items-center justify-center py-12">
            <UIcon :name="appConfig.ui.icons.loading"
                   class="size-5 animate-spin text-dimmed" />
        </div>

        <template v-else-if="loaded">
            <UTable :data="pageRows"
                    :columns="columns"
                    :loading="loading">
                <template v-for="field in fields"
                          :key="field.key"
                          #[`${field.key}-header`]>
                    {{ field.key }}<span v-if="field.required"
                                         class="ms-0.5 text-error">*</span>
                </template>

                <template v-for="field in editableFields"
                          :key="field.key"
                          #[`${field.key}-cell`]="{ row }">
                    <USwitch v-if="field.widget === 'switch'"
                             :model-value="row.original[field.key] === true"
                             @update:model-value="update(row.index, field, $event)" />
                    <USelect v-else-if="field.widget === 'select'"
                             :model-value="enumValue(row.original[field.key])"
                             :items="enumItems(field)"
                             size="sm"
                             class="w-full min-w-32"
                             @update:model-value="update(row.index, field, $event)" />
                    <UInputDate v-else-if="field.widget === 'date'"
                                :model-value="calendarDate(row.original[field.key])"
                                size="sm"
                                @update:model-value="update(row.index, field, $event ? String($event) : null)" />
                    <UInput v-else-if="field.widget === 'number'"
                            type="number"
                            :model-value="numberValue(row.original[field.key]) ?? undefined"
                            :step="field.property.type === 'integer' ? 1 : 'any'"
                            size="sm"
                            class="w-full min-w-24"
                            @update:model-value="update(row.index, field, numberValue($event))" />
                    <UInput v-else
                            :model-value="textValue(field, row.original[field.key]) ?? ''"
                            :placeholder="field.widget === 'datetime' ? '2026-01-31T09:00:00Z' : undefined"
                            size="sm"
                            class="w-full min-w-40"
                            @update:model-value="update(row.index, field, textValue(field, $event))" />
                </template>

                <template #actions-cell="{ row }">
                    <div class="flex justify-end opacity-0 transition-opacity group-hover/row:opacity-100 focus-within:opacity-100 pointer-coarse:opacity-100">
                        <UButton :icon="appConfig.icons.delete"
                                 color="neutral"
                                 variant="ghost"
                                 size="sm"
                                 aria-label="Delete row"
                                 @click="deleteRow(row.index)" />
                    </div>
                </template>

                <template #empty>
                    <UEmpty variant="naked"
                            size="sm"
                            :icon="appConfig.icons.rows"
                            title="No rows yet" />
                </template>
            </UTable>

            <TableFooter v-model:page="page"
                         :total="rows.length"
                         :page-size="PAGE_SIZE" />
        </template>

        <ExecutionsRunModal v-if="runDialogOpen"
                            v-model:open="runDialogOpen"
                            :target="context.source"
                            partitioned />
    </div>
</template>
