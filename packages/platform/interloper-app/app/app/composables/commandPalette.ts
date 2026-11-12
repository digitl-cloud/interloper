import type { CommandPaletteGroup, CommandPaletteItem } from '@nuxt/ui'
import type { Page } from '~/composables/api'
import type { Run } from '~/types/run'
import type { AdminOrganisation } from '~/types/admin'

export function useCommandPalette() {
    const { apiFetch } = useOrgApi()
    const userStore = useUserStore()
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()
    const orgStore = useOrganisationStore()
    const adminStore = useAdminStore()
    const colorMode = useColorMode()
    const appConfig = useAppConfig()
    const { icons } = appConfig
    const { open: agentOpen } = useAgentPanel()
    const destinations = useNavDestinations()
    const organizationViews = useOrganizationViews()
    const assetNames = useAssetDisplayName()
    const assetIcons = useAssetIcon()

    const open = ref(false)
    const searchTerm = ref('')
    const loading = computed(() => componentsStore.loading)

    const recentRuns = ref<Run[]>([])
    const adminOrgs = ref<AdminOrganisation[]>([])

    // Refresh data on open so search results are current, not a stale snapshot.
    watch(open, (isOpen) => {
        if (!isOpen) return
        searchTerm.value = ''
        if (!componentsStore.loading) componentsStore.fetchAll()
        apiFetch<Page<Run>>('/runs?limit=5').then((page) => {
            recentRuns.value = page.items
        }).catch(() => {})
        if (userStore.isSuperAdmin) {
            adminStore.listOrganisations().then((orgs) => {
                adminOrgs.value = orgs
            }).catch(() => {})
        }
    })

    const groups = computed<CommandPaletteGroup[]>(() => {
        const close = () => {
            open.value = false
        }
        const toItem = (page: NavPage): CommandPaletteItem => ({ ...page, onSelect: close })

        const actionItems: CommandPaletteItem[] = [
            toItem({ label: 'New source…', icon: appConfig.ui.icons.plus, to: `${orgStore.path(kindPath('source'))}?new=1` }),
            ...componentsStore.byKind('job').map(job =>
                toItem({ label: `Run job: ${job.name ?? job.key}`, icon: icons.run, to: `${orgStore.path(kindPath('job'))}?run=${job.id}` })),
            {
                label: colorMode.value === 'dark' ? 'Switch to light mode' : 'Switch to dark mode',
                icon: colorMode.value === 'dark' ? appConfig.ui.icons.light : appConfig.ui.icons.dark,
                onSelect: () => {
                    colorMode.preference = colorMode.value === 'dark' ? 'light' : 'dark'
                    close()
                },
            },
        ]
        if (userStore.agentAvailable) {
            actionItems.push({
                label: agentOpen.value ? 'Close agent panel' : 'Open agent panel',
                icon: icons.agent,
                onSelect: () => {
                    agentOpen.value = !agentOpen.value
                    close()
                },
            })
        }

        const settingsItems: CommandPaletteItem[] = [
            ...organizationViews.value.map(view => toItem({ ...view, label: `Organization ${view.label.toLowerCase()}` })),
        ]
        if (userStore.isSuperAdmin)
            settingsItems.push(toItem({ label: 'Platform admin', icon: icons.admin, to: '/admin' }))
        for (const membership of userStore.user?.memberships ?? []) {
            if (membership.id === orgStore.organisation?.id) continue
            settingsItems.push(toItem({ label: `Switch to ${membership.name}`, icon: icons.organisation, to: `/${membership.key}` }))
        }

        const runItems: CommandPaletteItem[] = recentRuns.value.map(run => ({
            label: targetLabel(run),
            suffix: `${statusMeta(run.status).label} · ${formatDate(run.created_at)}`,
            icon: statusIcon(run.status),
            to: orgStore.path(`/executions/runs/${run.id}`),
            onSelect: close,
        }))

        const searching = Boolean(searchTerm.value.trim())

        return [
            { id: 'pages', label: 'Pages', items: destinations.value.flatMap(page => (page.views ?? [page]).map(toItem)) },
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
                        icon: icons.admin,
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
                    suffix: [
                        catalogStore.getSourceDefinition(source.key)?.name,
                        source.discriminator !== source.name ? source.discriminator : null,
                    ].filter(Boolean).join(' · ') || undefined,
                    icon: componentIcon(source.key),
                    to: orgStore.path(kindPath('source')),
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
                        icon: assetIcons.value.get(asset.id) ?? icons.asset,
                        to: `${orgStore.path('/graph')}?select=${asset.id}`,
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
                    to: orgStore.path(kindPath('destination')),
                    onSelect: close,
                })),
            },
            ...catalogStore.resourceKinds.map(kind => ({
                id: `entity-${kind}`,
                label: kindLabel(kind),
                items: componentsStore.byKind(kind).map(resource => ({
                    label: resource.name ?? resource.key,
                    icon: resourceKindIcon(kind),
                    to: orgStore.path(kindPath(kind)),
                    onSelect: close,
                })),
            })),
            {
                id: 'entity-jobs',
                label: 'Jobs',
                items: componentsStore.byKind('job').map(job => ({
                    label: job.name ?? job.key,
                    icon: icons.job,
                    to: orgStore.path(kindPath('job')),
                    onSelect: close,
                })),
            },
        ]

        return groups.filter(group => group.items && group.items.length > 0)
    }

    return { open, searchTerm, loading, groups }
}
