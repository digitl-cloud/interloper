<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'

definePageMeta({ title: 'Resources' })

const UIcon = resolveComponent('UIcon')

const route = useRoute()
const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()

const kind = computed(() => route.params.kind as string)
const pageTitle = computed(() => {
    const k = kind.value
    return k.charAt(0).toUpperCase() + k.slice(1) + 's'
})

/** Available type definitions for this resource kind. */
const definitions = computed(() => catalogStore.definitionsForKind(kind.value))

/** Resources filtered by current kind. */
const resources = computed(() => componentsStore.byKind(kind.value))

function typeName(key: string): string {
    return catalogStore.catalog[key]?.name ?? key
}

const stepperRef = ref<any>(null)

const {
    open: drawerOpen,
    editing: editingResource,
    presetTypeKey,
    openCreate: handleCreate,
    openCreateWithType: handleCreateFromCatalog,
    openEdit: handleEdit,
} = useWizardDrawer<ComponentRecord>()

// Re-fetch when kind changes
watch(kind, () => componentsStore.fetchAll([kind.value]), { immediate: true })

const columns: TableColumn<ComponentRecord>[] = [
    { accessorKey: 'select' as any, header: '' },
    { accessorKey: 'name', header: 'Name' },
    {
        accessorKey: 'key',
        header: 'Type',
        cell: ({ row }) => h('span', { class: 'flex items-center gap-1.5 text-muted' }, [
            h(UIcon, { name: componentIcon(row.original.key), class: 'size-4 shrink-0' }),
            typeName(row.original.key),
        ]),
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        accessorFn: (row: ComponentRecord) => row.created_at ? formatDate(row.created_at) : '-',
    },
]

const toast = useToast()

async function handleDelete(ids: string[]) {
    try {
        await componentsStore.remove(ids)
        toast.add({ title: `${ids.length} resource(s) deleted`, color: 'success' })
    }
    catch (e) {
        toast.add(inUseToast(e, pageTitle.value.slice(0, -1)) ?? { title: 'Failed to delete resource', color: 'error' })
    }
}

function handleSaved() {
    componentsStore.fetchAll([kind.value])
    drawerOpen.value = false
}

// ── Empty state ──

const KIND_ICONS: Record<string, string> = {
    connection: 'i-lucide-key-round',
    config: 'i-lucide-settings',
}

const EMPTY_COPY: Record<string, { hero: string, catalogTitle: string, catalogDesc: string }> = {
    connection: {
        hero: 'A connection is a securely-stored credential — an OAuth token, API key or service account — that '
            + 'Interloper uses to authenticate with a platform. Create one, and every source from that provider '
            + 'can reuse it. Credentials are encrypted at rest and never leave your cloud.',
        catalogTitle: 'Connections Catalog',
        catalogDesc: 'Pick a platform to store a reusable credential — every source from that provider can share it.',
    },
}

const emptyCopy = computed(() => EMPTY_COPY[kind.value] ?? {
    hero: `Create your first ${kind.value} to get started.`,
    catalogTitle: 'Available types',
    catalogDesc: `Pick a type to create a new ${kind.value}.`,
})
</script>

<template>
    <div>
        <div class="flex flex-col flex-1 min-h-0">
            <DataTable :columns="columns"
                       :data="resources"
                       :loading="componentsStore.loading"
                       :search-placeholder="`Search ${pageTitle.toLowerCase()}...`"
                       @delete="handleDelete"
                       @edit="handleEdit">
                <template #toolbar>
                    <UButton icon="i-lucide-plus"
                             :label="`New ${kind}`"
                             @click="handleCreate" />
                </template>

                <template #empty>
                    <EmptyState :icon="KIND_ICONS[kind] ?? 'i-lucide-box'"
                                :title="`No ${pageTitle.toLowerCase()} yet`"
                                :description="emptyCopy.hero">
                        <UButton icon="i-lucide-plus"
                                 :label="`New ${kind}`"
                                 class="mt-5"
                                 @click="handleCreate" />
                    </EmptyState>

                    <div class="mt-9 mb-3.5">
                        <h2 class="text-lg font-bold tracking-[-0.015em] text-highlighted">
                            {{ emptyCopy.catalogTitle }}
                        </h2>
                        <p class="text-sm text-muted mt-1.5">{{ emptyCopy.catalogDesc }}</p>
                    </div>
                    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                        <CatalogCard v-for="def in definitions"
                                     :key="def.key"
                                     variant="compact"
                                     :icon="componentIcon(def.key)"
                                     :title="def.name"
                                     :caption="def.provider"
                                     @click="handleCreateFromCatalog(def.key)" />
                    </div>

                    <div v-if="kind === 'connection'"
                         class="flex items-center gap-3 border border-default rounded-[14px] px-5 py-4 bg-(--ui-bg-band) mt-6">
                        <UIcon name="i-lucide-info"
                               class="size-5 text-primary shrink-0" />
                        <div class="flex-1 text-[13.5px] text-toned leading-normal">
                            You can also create a connection inline while adding a source — whichever you reach first.
                            Ready to connect data?
                            <NuxtLink to="/sources"
                                      class="text-primary font-semibold">Browse sources →</NuxtLink>
                        </div>
                    </div>
                </template>
            </DataTable>
        </div>

        <WizardDrawer v-model:open="drawerOpen"
                      modal
                      :default-title="`New ${kind}`"
                      :description="`${editingResource ? 'Edit' : 'Configure a new'} ${kind}`"
                      :stepper="stepperRef">
            <ResourcesStepper v-if="drawerOpen"
                              :key="editingResource?.id ?? 'new'"
                              ref="stepperRef"
                              :kind="kind"
                              :initial-type-key="presetTypeKey"
                              :definitions="definitions"
                              :resource="editingResource"
                              @created="handleSaved"
                              @updated="handleSaved" />
        </WizardDrawer>
    </div>
</template>
