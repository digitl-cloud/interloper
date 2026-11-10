<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { AuthorisedApp, PersonalAccessToken } from '~/types/token'

definePageMeta({ layout: 'settings' })

const UBadge = resolveComponent('UBadge')

const route = useRoute()
const router = useRouter()
const userStore = useUserStore()
const toast = useToast()
const { confirm } = useConfirm()
const { apiFetch, fetchAll } = useApi()

const tokens = ref<PersonalAccessToken[]>([])
const loading = ref(false)
const createOpen = ref(false)
const apps = ref<AuthorisedApp[]>([])
const appsLoading = ref(false)

type Tab = 'signin' | 'tokens' | 'apps'

// Unknown values, and none, are the sign-in pane.
const activeTab = computed<Tab>(() => {
    const tab = route.query.tab
    return tab === 'tokens' || tab === 'apps' ? tab : 'signin'
})

onMounted(() => {
    if (!route.query.tab) {
        router.replace({ query: { tab: 'signin' } })
    }
    loadTokens()
    loadApps()
})

const tabs = computed<PageTab[]>(() => [
    { label: 'Sign in', icon: 'i-lucide-log-in', to: { query: { tab: 'signin' } }, active: activeTab.value === 'signin' },
    {
        label: 'Personal Access Tokens',
        icon: 'i-lucide-key',
        badge: loading.value ? undefined : tokens.value.length,
        to: { query: { tab: 'tokens' } },
        active: activeTab.value === 'tokens',
    },
    {
        label: 'Authorised apps',
        icon: 'i-lucide-plug',
        badge: appsLoading.value ? undefined : apps.value.length,
        to: { query: { tab: 'apps' } },
        active: activeTab.value === 'apps',
    },
])

async function loadTokens() {
    loading.value = true
    try {
        tokens.value = await fetchAll<PersonalAccessToken>('/tokens')
    }
    catch (err) {
        console.error('[Settings] Failed to load tokens', err)
    }
    finally {
        loading.value = false
    }
}

async function loadApps() {
    appsLoading.value = true
    try {
        apps.value = await apiFetch<AuthorisedApp[]>('/auth/apps')
    }
    catch (err) {
        console.error('[Settings] Failed to load authorised apps', err)
    }
    finally {
        appsLoading.value = false
    }
}

type TokenStatus = 'active' | 'expired' | 'revoked'

function tokenStatus(token: PersonalAccessToken): TokenStatus {
    if (token.revoked_at) return 'revoked'
    if (token.expires_at && new Date(token.expires_at) < new Date()) return 'expired'
    return 'active'
}

const STATUS_BADGES: Record<TokenStatus, { label: string, color: string }> = {
    active: { label: 'Active', color: 'success' },
    expired: { label: 'Expired', color: 'neutral' },
    revoked: { label: 'Revoked', color: 'neutral' },
}

async function revokeToken(token: PersonalAccessToken) {
    const confirmed = await confirm({
        title: 'Revoke token',
        description: 'Clients authenticating with {subject} will immediately lose access. '
            + 'This action cannot be undone.',
        subject: { name: token.name, icon: 'i-lucide-key' },
        confirmLabel: 'Revoke',
        confirmColor: 'error',
    })
    if (!confirmed) return

    try {
        await apiFetch(`/tokens/${token.id}`, { method: 'DELETE' })
        toast.add({ title: `${token.name} revoked`, color: 'success' })
        await loadTokens()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to revoke token'))
    }
}

function rowActions(token: PersonalAccessToken): DropdownMenuItem[][] {
    if (tokenStatus(token) === 'revoked') return []
    return [
        [
            {
                label: 'Revoke token',
                icon: 'i-lucide-shield-off',
                color: 'error' as const,
                onSelect: () => revokeToken(token),
            },
        ],
    ]
}

async function revokeApp(app: AuthorisedApp) {
    const confirmed = await confirm({
        title: 'Revoke access',
        description: '{subject} will immediately lose access to every organisation you belong to. '
            + 'To use it again, you will have to approve it again.',
        subject: { name: app.name, icon: 'i-lucide-plug' },
        confirmLabel: 'Revoke',
        confirmColor: 'error',
    })
    if (!confirmed) return

    try {
        await apiFetch(`/auth/apps/${encodeURIComponent(app.id)}`, { method: 'DELETE' })
        toast.add({ title: `${app.name} revoked`, color: 'success' })
        await loadApps()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to revoke access'))
    }
}

function appActions(app: AuthorisedApp): DropdownMenuItem[][] {
    return [
        [
            {
                label: 'Revoke access',
                icon: 'i-lucide-shield-off',
                color: 'error' as const,
                onSelect: () => revokeApp(app),
            },
        ],
    ]
}

/** The hosts an app returns to, port included; a URI with no host stands for itself. */
function appHosts(app: AuthorisedApp): string[] {
    const hosts = app.redirect_uris.map((uri) => {
        try {
            return new URL(uri).host || uri
        }
        catch {
            return uri
        }
    })
    return [...new Set(hosts)]
}

const appColumns: TableColumn<AuthorisedApp>[] = [
    {
        accessorKey: 'name',
        header: 'Name',
        cell: ({ row }) => h('span', { class: 'font-semibold text-highlighted' }, row.original.name),
    },
    {
        id: 'returns_to',
        header: 'Returns to',
        accessorFn: row => appHosts(row).join(' '),
        cell: ({ row }) => h('div', { class: 'flex flex-wrap gap-1' },
            appHosts(row.original).map(host => h(UBadge, {
                label: host,
                color: 'neutral',
                variant: 'soft',
                class: 'font-mono',
            })),
        ),
    },
    {
        accessorKey: 'created_at',
        header: 'Registered',
        cell: ({ row }) => h('span', { class: 'text-muted' }, row.original.created_at
            ? formatDay(row.original.created_at)
            : 'Unknown'),
    },
]

const columns: TableColumn<PersonalAccessToken>[] = [
    {
        accessorKey: 'name',
        header: 'Name',
        cell: ({ row }) => h('span', {
            class: tokenStatus(row.original) === 'active'
                ? 'font-semibold text-highlighted'
                : 'font-semibold text-dimmed line-through',
        }, row.original.name),
    },
    {
        accessorKey: 'token_prefix',
        header: 'Token',
        cell: ({ row }) => h(UBadge, {
            label: `${row.original.token_prefix}…`,
            color: 'neutral',
            variant: 'soft',
            class: 'font-mono',
        }),
    },
    {
        accessorKey: 'last_used_at',
        header: 'Last used',
        cell: ({ row }) => h('span', { class: 'text-muted' }, row.original.last_used_at
            ? `${timeSince(new Date(row.original.last_used_at))} ago`
            : 'Never'),
    },
    {
        accessorKey: 'expires_at',
        header: 'Expires',
        cell: ({ row }) => h('span', { class: 'text-muted' }, row.original.expires_at
            ? formatDay(row.original.expires_at)
            : 'Never'),
    },
    {
        id: 'status',
        header: 'Status',
        accessorFn: row => tokenStatus(row),
        cell: ({ row }) => {
            const badge = STATUS_BADGES[tokenStatus(row.original)]
            return h(UBadge, { label: badge.label, color: badge.color, variant: 'subtle' })
        },
    },
]
</script>

<template>
    <UDashboardPanel id="authentication">
        <template #header>
            <AppNavbar title="Authentication" />
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="tabs" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <UCard v-if="activeTab === 'signin'"
                   title="Sign in"
                   class="mx-auto w-full max-w-3xl">
                <div class="flex items-center gap-3">
                    <div class="flex size-7 shrink-0 items-center justify-center rounded-md bg-elevated">
                        <UIcon name="i-devicon-google"
                               class="size-3.5" />
                    </div>
                    <div class="min-w-0 flex-1">
                        <div class="text-sm font-medium text-highlighted">Google</div>
                        <div class="mt-0.5 text-sm text-muted">{{ userStore.user?.email }}</div>
                    </div>
                    <UBadge label="Connected"
                            color="success"
                            variant="subtle"
                            class="shrink-0" />
                </div>
            </UCard>

            <DataTable v-else-if="activeTab === 'tokens'"
                       :columns="columns"
                       :data="tokens"
                       :loading="loading"
                       :row-actions="rowActions"
                       no-actions
                       no-row-click
                       search-placeholder="Search tokens...">
                <template #actions>
                    <UButton icon="i-lucide-plus"
                             label="New token"
                             @click="createOpen = true" />
                </template>
            </DataTable>

            <DataTable v-else
                       :columns="appColumns"
                       :data="apps"
                       :loading="appsLoading"
                       :row-actions="appActions"
                       no-actions
                       no-row-click
                       search-placeholder="Search apps..." />

            <SettingsTokenCreateModal v-model:open="createOpen"
                                      @created="loadTokens" />
        </template>
    </UDashboardPanel>
</template>
