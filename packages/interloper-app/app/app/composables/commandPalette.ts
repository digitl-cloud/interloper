import type { CommandPaletteGroup, CommandPaletteItem } from '@nuxt/ui'

export function useCommandPalette() {
    const userStore = useUserStore()
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()
    const colorMode = useColorMode()
    const { open: agentOpen } = useAgentPanel()
    const sections = useNavSections()
    const assetNames = useAssetDisplayName()
    const assetIcons = useAssetIcon()

    const open = ref(false)
    const searchTerm = ref('')
    const loading = computed(() => componentsStore.loading)

    // Refresh entity data on open so search results are current, not a stale snapshot.
    watch(open, (isOpen) => {
        if (!isOpen) return
        searchTerm.value = ''
        if (!componentsStore.loading) componentsStore.fetchAll()
    })

    defineShortcuts({
        meta_k: {
            usingInput: true,
            handler: () => {
                open.value = !open.value
            },
        },
    })

    const groups = computed<CommandPaletteGroup[]>(() => {
        const close = () => {
            open.value = false
        }
        const toItem = (page: NavPage): CommandPaletteItem => ({ ...page, onSelect: close })

        const actionItems: CommandPaletteItem[] = [
            toItem({ label: 'New source…', icon: 'i-lucide-plus', to: '/sources?new=1' }),
            ...componentsStore.byKind('job').map(job =>
                toItem({ label: `Run job: ${job.name ?? job.key}`, icon: 'i-lucide-play', to: `/jobs?run=${job.id}` })),
            {
                label: colorMode.value === 'dark' ? 'Switch to light mode' : 'Switch to dark mode',
                icon: colorMode.value === 'dark' ? 'i-lucide-sun' : 'i-lucide-moon',
                onSelect: () => {
                    colorMode.preference = colorMode.value === 'dark' ? 'light' : 'dark'
                    close()
                },
            },
        ]
        if (userStore.agentAvailable) {
            actionItems.push({
                label: agentOpen.value ? 'Close agent panel' : 'Open agent panel',
                icon: 'i-lucide-sparkles',
                onSelect: () => {
                    agentOpen.value = !agentOpen.value
                    close()
                },
            })
        }

        const settingsItems: CommandPaletteItem[] = [
            toItem({ label: 'Organization', icon: 'i-lucide-building-2', to: '/organization' }),
        ]
        if (userStore.isSuperAdmin)
            settingsItems.push(toItem({ label: 'Platform admin', icon: 'i-lucide-shield', to: '/admin' }))

        return [
            { id: 'pages', label: 'Pages', items: sections.value.flatMap(section => section.pages.map(toItem)) },
            // Entity groups only join in once the user types — the empty palette
            // stays a compact quick-nav instead of a dump of the whole collection.
            ...(searchTerm.value.trim() ? entityGroups(close) : []),
            { id: 'actions', label: 'Actions', items: actionItems },
            { id: 'settings', label: 'Settings', items: settingsItems },
        ]
    })

    /** One group per component kind, built from the org's collection. Empty groups are dropped. */
    function entityGroups(close: () => void): CommandPaletteGroup[] {
        const sources = componentsStore.byKind('source')

        const groups: CommandPaletteGroup[] = [
            {
                id: 'entity-sources',
                label: 'Sources',
                items: sources.map(source => ({
                    label: source.name ?? source.key,
                    suffix: catalogStore.getSourceDefinition(source.key)?.name,
                    icon: componentIcon(source.key),
                    to: '/sources',
                    onSelect: close,
                })),
            },
            {
                id: 'entity-assets',
                label: 'Assets',
                items: sources.flatMap(source => source.children.map((asset) => {
                    const names = assetNames.value.get(asset.id)
                    return {
                        label: names?.assetName ?? asset.name ?? asset.key,
                        suffix: names?.sourceName,
                        icon: assetIcons.value.get(asset.id) ?? 'i-lucide-box',
                        to: `/graph?select=${asset.id}`,
                        onSelect: close,
                    }
                })),
            },
            {
                id: 'entity-destinations',
                label: 'Destinations',
                items: componentsStore.byKind('destination').map(destination => ({
                    label: destination.name ?? destination.key,
                    icon: componentIcon(destination.key),
                    to: '/destinations',
                    onSelect: close,
                })),
            },
            ...catalogStore.resourceKinds.map(kind => ({
                id: `entity-${kind}`,
                label: kindLabel(kind),
                items: componentsStore.byKind(kind).map(resource => ({
                    label: resource.name ?? resource.key,
                    icon: RESOURCE_KIND_ICONS[kind] ?? 'i-lucide-box',
                    to: `/resources/${kind}`,
                    onSelect: close,
                })),
            })),
            {
                id: 'entity-jobs',
                label: 'Jobs',
                items: componentsStore.byKind('job').map(job => ({
                    label: job.name ?? job.key,
                    icon: 'i-lucide-calendar-clock',
                    to: '/jobs',
                    onSelect: close,
                })),
            },
        ]

        return groups.filter(group => group.items && group.items.length > 0)
    }

    return { open, searchTerm, loading, groups }
}
