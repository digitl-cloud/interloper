import type { CommandPaletteGroup, CommandPaletteItem } from '@nuxt/ui'
import type { Run } from '~/types/run'
import type { Organisation } from '~/types/organisation'
import type { AdminOrganisation } from '~/types/admin'

const RUN_STATUS_ICONS: Record<string, string> = {
    success: 'i-lucide-circle-check',
    failed: 'i-lucide-circle-x',
    running: 'i-lucide-loader-circle',
    dispatched: 'i-lucide-loader-circle',
    canceled: 'i-lucide-circle-slash',
}

export function useCommandPalette() {
    const { apiFetch } = useApi()
    const userStore = useUserStore()
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()
    const orgStore = useOrganisationStore()
    const adminStore = useAdminStore()
    const colorMode = useColorMode()
    const { open: agentOpen } = useAgentPanel()
    const { switchToOrg } = useOrgSwitch()
    const sections = useNavSections()
    const assetNames = useAssetDisplayName()
    const assetIcons = useAssetIcon()

    const open = ref(false)
    const searchTerm = ref('')
    const loading = computed(() => componentsStore.loading)

    const recentRuns = ref<Run[]>([])
    const userOrgs = ref<Organisation[]>([])
    const adminOrgs = ref<AdminOrganisation[]>([])

    /** component_id → display name, covering source-owned children too (targets can be assets). */
    const runTargetNames = computed(() => {
        const map = new Map<string, string>()
        for (const component of componentsStore.components) {
            map.set(component.id, component.name ?? component.key)
            for (const child of component.children) map.set(child.id, child.name ?? child.key)
        }
        return map
    })

    // Refresh data on open so search results are current, not a stale snapshot.
    watch(open, (isOpen) => {
        if (!isOpen) return
        searchTerm.value = ''
        if (!componentsStore.loading) componentsStore.fetchAll()
        apiFetch<Run[]>('/runs?limit=5').then((runs) => {
            recentRuns.value = runs
        }).catch(() => {})
        orgStore.fetchOrganisations().then((orgs) => {
            userOrgs.value = orgs
        }).catch(() => {})
        if (userStore.isSuperAdmin) {
            adminStore.listOrganisations().then((orgs) => {
                adminOrgs.value = orgs
            }).catch(() => {})
        }
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
        for (const org of userOrgs.value) {
            if (org.id === orgStore.organisation?.id) continue
            settingsItems.push({
                label: `Switch to ${org.name}`,
                icon: 'i-lucide-building-2',
                onSelect: () => {
                    close()
                    switchToOrg(org)
                },
            })
        }

        const runItems: CommandPaletteItem[] = recentRuns.value.map(run => ({
            label: runTargetNames.value.get(run.component_id ?? '') ?? 'Deleted target',
            suffix: `${statusLabel(run.status)} · ${formatDate(run.created_at)}`,
            icon: RUN_STATUS_ICONS[run.status] ?? 'i-lucide-circle-dashed',
            to: `/executions/runs/${run.id}`,
            onSelect: close,
        }))

        const searching = Boolean(searchTerm.value.trim())

        return [
            { id: 'pages', label: 'Pages', items: sections.value.flatMap(section => section.pages.map(toItem)) },
            // Entity groups only join in once the user types — the empty palette
            // stays a compact quick-nav instead of a dump of the whole collection.
            ...(searching ? entityGroups(close) : []),
            ...(runItems.length ? [{ id: 'recent-runs', label: 'Recent runs', items: runItems }] : []),
            { id: 'actions', label: 'Actions', items: actionItems },
            // Cross-org admin jumps are search-only: super admins may oversee many
            // orgs, and browsing them belongs on /admin.
            ...(searching && adminOrgs.value.length
                ? [{
                    id: 'admin-organisations',
                    label: 'Administration',
                    items: adminOrgs.value.map(org => ({
                        label: `Admin: ${org.name}`,
                        icon: 'i-lucide-shield',
                        to: `/admin/organisations/${org.id}`,
                        onSelect: close,
                    })),
                }]
                : []),
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
