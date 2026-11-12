<script setup lang="ts">
/**
 * Placeholder for pages that need at least one source (sources, catalog):
 * the empty state with its "New source" action, then the connector catalog
 * filtered by tag.
 */
const emit = defineEmits<{
    /** Open the source wizard on the type step. */
    create: []
    /** Open the source wizard with this connector preselected. */
    createType: [key: string]
}>()

const catalogStore = useCatalogStore()
const appConfig = useAppConfig()

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
            maturity: d.maturity,
            caption: d.provider,
            description: d.description,
            chips: [
                ...(d.tags ?? []).map(t => ({ label: t })),
                { icon: appConfig.icons.asset, label: `${d.assets.length} assets` },
            ],
        },
    })))
</script>

<template>
    <div class="flex flex-col gap-4 sm:gap-6">
        <UEmpty :icon="appConfig.icons.source"
                title="No sources yet"
                description="A source is a connector Interloper pulls from. Authenticate once and it materialises its assets into your own warehouse."
                :actions="[{ label: 'New source', icon: appConfig.ui.icons.plus, onClick: () => emit('create') }]" />

        <section class="flex flex-col gap-4">
            <UPageCard title="Connector catalog"
                       description="Every source Interloper can pull from. Pick one to set it up."
                       variant="naked"
                       orientation="horizontal" />

            <div class="flex flex-wrap items-center gap-2">
                <UTabs v-model="connectorFilter"
                       :items="connectorFilters"
                       variant="pill"
                       size="xs"
                       :content="false" />
                <span class="ml-auto text-xs text-dimmed tabular-nums">
                    {{ connectorCards.length }} of {{ catalogStore.sourceDefinitions.length }} connectors
                </span>
            </div>

            <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                <CatalogCard v-for="card in connectorCards"
                             :key="card.key"
                             variant="rich"
                             v-bind="card.props"
                             @click="emit('createType', card.key)" />
            </div>
        </section>
    </div>
</template>
