import type { AssetDefinition, Catalog, ComponentDefinition, DestinationDefinition, SourceDefinition } from '~/types/catalog'

export const useCatalogStore = defineStore('catalog', () => {
    const { apiFetch } = useApi()

    /**********************
     * State
     **********************/
    const catalog = ref<Catalog>({})
    const resourceKinds = ref<string[]>([])
    const oauthProviders = ref<OAuthProviderInfo[]>([])
    const loaded = ref(false)

    /**********************
     * Getters
     **********************/
    const byKind = computed(() => {
        const grouped: Record<string, ComponentDefinition[]> = {}
        for (const defn of Object.values(catalog.value)) {
            const list = grouped[defn.kind] ??= []
            list.push(defn)
        }
        return grouped
    })

    /**********************
     * Actions
     **********************/
    async function fetchCatalog() {
        const [cat, kinds, providers] = await Promise.all([
            apiFetch<Catalog>('/catalog'),
            apiFetch<string[]>('/catalog/resource-kinds'),
            apiFetch<OAuthProviderInfo[]>('/oauth/providers'),
        ])
        catalog.value = cat
        resourceKinds.value = kinds
        oauthProviders.value = providers
        loaded.value = true
    }

    function definitionsForKind(kind: string): ComponentDefinition[] {
        return byKind.value[kind] ?? []
    }

    /** All source definitions from the catalog. */
    const sourceDefinitions = computed<SourceDefinition[]>(() =>
        definitionsForKind('source') as SourceDefinition[],
    )

    /** Get a source definition by key. */
    function getSourceDefinition(key: string): SourceDefinition | undefined {
        return sourceDefinitions.value.find(s => s.key === key)
    }

    /** All standalone asset definitions from the catalog. */
    const assetDefinitions = computed<AssetDefinition[]>(() =>
        definitionsForKind('asset') as AssetDefinition[],
    )

    /** Get an asset definition by key (bare or qualified). */
    function getAssetDefinition(key: string): AssetDefinition | undefined {
        // Try direct catalog lookup first
        const direct = catalog.value[key]
        if (direct && direct.kind === 'asset') return direct as AssetDefinition
        // Search source definitions for nested assets
        for (const src of sourceDefinitions.value) {
            const found = src.assets?.find(a => a.key === key || `${src.key}.${a.key}` === key)
            if (found) return found
        }
        return undefined
    }

    /** All destination definitions from the catalog. */
    const destinationDefinitions = computed<DestinationDefinition[]>(() =>
        definitionsForKind('destination') as DestinationDefinition[],
    )

    /** Get a destination definition by key. */
    function getDestinationDefinition(key: string): DestinationDefinition | undefined {
        return destinationDefinitions.value.find(d => d.key === key)
    }

    /** Check if an OAuth provider is available (configured on the server). */
    function isOAuthProviderAvailable(provider: string): boolean {
        return oauthProviders.value.some(p => p.key === provider)
    }

    /** Get the public info for an OAuth provider. */
    function getOAuthProvider(provider: string): OAuthProviderInfo | undefined {
        return oauthProviders.value.find(p => p.key === provider)
    }

    return {
        catalog,
        resourceKinds,
        oauthProviders,
        loaded,
        byKind,
        sourceDefinitions,
        assetDefinitions,
        destinationDefinitions,
        fetchCatalog,
        definitionsForKind,
        getSourceDefinition,
        getAssetDefinition,
        getDestinationDefinition,
        isOAuthProviderAvailable,
        getOAuthProvider,
    }
})
