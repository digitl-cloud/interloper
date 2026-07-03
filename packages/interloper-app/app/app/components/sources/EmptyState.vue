<script setup lang="ts">
/**
 * Placeholder for pages that need at least one source (sources, catalog):
 * design hero + the connector catalog with tag filters.
 */
const emit = defineEmits<{
    /** Open the source wizard on the type step. */
    create: []
    /** Open the source wizard with this connector preselected. */
    createType: [key: string]
}>()

const catalogStore = useCatalogStore()

/** Filter pills: the catalog's real tags (first tag per connector). */
const connectorFilters = computed(() => {
    const tags = new Set<string>()
    for (const d of catalogStore.sourceDefinitions) tags.add(d.tags?.[0] ?? 'Other')
    return ['All', ...tags].map(t => ({ label: t, value: t }))
})
const connectorFilter = ref('All')

/** Card view-models, so chip arrays keep stable identities across renders. */
const connectorCards = computed(() => catalogStore.sourceDefinitions
    .filter(d => connectorFilter.value === 'All' || (d.tags?.[0] ?? 'Other') === connectorFilter.value)
    .map(d => ({
        key: d.key,
        props: {
            icon: componentIcon(d.key),
            title: d.name,
            caption: d.provider,
            description: d.description,
            chips: [
                ...(d.tags ?? []).map(t => ({ label: t })),
                { icon: 'i-lucide-layers', label: `${d.assets.length} assets` },
            ],
        },
    })))
</script>

<template>
    <div>
        <EmptyState icon="i-lucide-plug"
                    title="No sources yet"
                    description="A source is a typed connector Interloper pulls from — authenticate once and it materializes its assets straight into your own warehouse. Pick one from the catalog below to add your first.">
            <UButton icon="i-lucide-plus"
                     label="New source"
                     class="mt-5"
                     @click="emit('create')" />
        </EmptyState>

        <div class="mt-9 mb-3.5">
            <h2 class="text-lg font-bold tracking-[-0.015em] text-highlighted">
                Connector catalog
            </h2>
            <p class="text-sm text-muted mt-1.5">
                Every source Interloper can pull from — pick what you need, authenticate once,
                and materialize into your own warehouse.
            </p>
        </div>

        <div class="flex items-center gap-2 mb-4 flex-wrap">
            <UTabs v-model="connectorFilter"
                   :items="connectorFilters"
                   variant="pill"
                   size="xs"
                   :content="false" />
            <span class="ml-auto font-mono text-xs text-dimmed">
                {{ connectorCards.length }} of {{ catalogStore.sourceDefinitions.length }} connectors
            </span>
        </div>

        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            <CatalogCard v-for="card in connectorCards"
                         :key="card.key"
                         variant="rich"
                         v-bind="card.props"
                         @click="emit('createType', card.key)" />
        </div>
    </div>
</template>
