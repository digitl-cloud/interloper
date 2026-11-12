<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { AuthorisedApp, PersonalAccessToken } from '~/types/token'

definePageMeta({ layout: 'settings' })

const UBadge = resolveComponent('UBadge')

const route = useRoute()
const router = useRouter()
const userStore = useUserStore()
const appConfig = useAppConfig()
const toast = useToast()
const { confirm } = useConfirm()
const { apiFetch, fetchAll } = useApi()

const tokens = ref<PersonalAccessToken[]>([])
const loading = ref(false)
const tokensError = ref<Error | null>(null)
const createOpen = ref(false)
const apps = ref<AuthorisedApp[]>([])
const appsLoading = ref(false)
const appsError = ref<Error | null>(null)

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
    { label: 'Sign in', icon: appConfig.icons.signIn, to: { query: { tab: 'signin' } }, active: activeTab.value === 'signin' },
    {
        label: 'Personal access tokens',
        icon: appConfig.icons.token,
        badge: loading.value ? undefined : tokens.value.length,
        to: { query: { tab: 'tokens' } },
        active: activeTab.value === 'tokens',
    },
    {
        label: 'Authorised apps',
        icon: appConfig.icons.app,
        badge: appsLoading.value ? undefined : apps.value.length,
        to: { query: { tab: 'apps' } },
        active: activeTab.value === 'apps',
    },
])

async function loadTokens() {
    loading.value = true
    tokensError.value = null
    try {
        tokens.value = await fetchAll<PersonalAccessToken>('/tokens')
    }
    catch (err) {
        tokensError.value = err as Error
    }
    finally {
        loading.value = false
    }
}

async function loadApps() {
    appsLoading.value = true
    appsError.value = null
    try {
        apps.value = await apiFetch<AuthorisedApp[]>('/auth/apps')
    }
    catch (err) {
        appsError.value = err as Error
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

const STATUS_BADGES: Record<TokenStatus, { label: string, color: 'success' | 'neutral' }> = {
    active: { label: 'Active', color: 'success' },
    expired: { label: 'Expired', color: 'neutral' },
    revoked: { label: 'Revoked', color: 'neutral' },
}

async function revokeToken(token: PersonalAccessToken) {
    const confirmed = await confirm({
        title: 'Revoke token',
        description: 'Clients authenticating with {subject} will immediately lose access. '
            + 'This action cannot be undone.',
        subject: { name: token.name, icon: appConfig.icons.token },
        confirmLabel: 'Revoke',
    })
    if (!confirmed) return

    try {
        await apiFetch(`/tokens/${token.id}`, { method: 'DELETE' })
        toast.add({ title: `${token.name} revoked`, color: 'success' })
        await loadTokens()
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't revoke the token"))
    }
}

function rowActions(token: PersonalAccessToken): DropdownMenuItem[][] {
    if (tokenStatus(token) === 'revoked') return []
    return [
        [
            {
                label: 'Revoke token',
                icon: appConfig.icons.cancel,
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
        subject: { name: app.name, icon: appConfig.icons.app },
        confirmLabel: 'Revoke',
    })
    if (!confirmed) return

    try {
        await apiFetch(`/auth/apps/${encodeURIComponent(app.id)}`, { method: 'DELETE' })
        toast.add({ title: `${app.name} revoked`, color: 'success' })
        await loadApps()
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't revoke the access"))
    }
}

function appActions(app: AuthorisedApp): DropdownMenuItem[][] {
    return [
        [
            {
                label: 'Revoke access',
                icon: appConfig.icons.cancel,
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
        cell: ({ row }) => h('div', { class: 'flex flex-col' }, [
            h('span', { class: 'font-medium text-highlighted' }, row.original.name),
            row.original.client_host
                ? h('span', { class: 'text-xs text-muted' }, `from ${row.original.client_host}`)
                : null,
        ]),
    },
    {
        id: 'returns_to',
        header: 'Returns to',
        accessorFn: row => appHosts(row).join(' '),
        cell: ({ row }) => h('div', { class: 'flex flex-wrap gap-1' },
            appHosts(row.original).map(host => h(UBadge, { label: host, color: 'neutral' }))),
    },
    {
        accessorKey: 'created_at',
        header: 'Registered',
        cell: ({ row }) => h('span', { class: 'text-muted tabular-nums' }, row.original.created_at
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
                ? 'font-medium text-highlighted'
                : 'font-medium text-dimmed line-through',
        }, row.original.name),
    },
    {
        accessorKey: 'token_prefix',
        header: 'Token',
        cell: ({ row }) => h(UBadge, { color: 'neutral' }, () =>
            h('span', { class: 'slashed-zero' }, `${row.original.token_prefix}…`)),
    },
    {
        accessorKey: 'last_used_at',
        header: 'Last used',
        cell: ({ row }) => h('span', { class: 'text-muted tabular-nums' }, row.original.last_used_at
            ? relativeTime(new Date(row.original.last_used_at))
            : 'Never'),
    },
    {
        accessorKey: 'expires_at',
        header: 'Expires',
        cell: ({ row }) => h('span', { class: 'text-muted tabular-nums' }, row.original.expires_at
            ? formatDay(row.original.expires_at)
            : 'Never'),
    },
    {
        id: 'status',
        header: 'Status',
        accessorFn: row => tokenStatus(row),
        cell: ({ row }) => {
            const badge = STATUS_BADGES[tokenStatus(row.original)]
            return h(UBadge, { label: badge.label, color: badge.color })
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
            <div v-if="activeTab === 'signin'"
                 class="mx-auto flex w-full max-w-2xl flex-col gap-4">
                <UPageCard variant="naked"
                           orientation="horizontal"
                           title="Sign in"
                           description="The accounts you sign in to Interloper with." />
                <UCard>
                    <UFormField label="Google"
                                :description="userStore.user?.email"
                                orientation="horizontal">
                        <template #label>
                            <span class="flex items-center gap-2">
                                <UIcon :name="appConfig.icons.google"
                                       class="size-4" />
                                Google
                            </span>
                        </template>
                        <UBadge label="Connected"
                                color="success" />
                    </UFormField>
                </UCard>
            </div>

            <DataTable v-else-if="activeTab === 'tokens'"
                       :columns="columns"
                       :data="tokens"
                       :loading="loading"
                       :error="tokensError"
                       :row-actions="rowActions"
                       no-actions
                       no-row-click
                       search-placeholder="Search tokens…"
                       @retry="loadTokens">
                <template #actions>
                    <UButton :icon="appConfig.ui.icons.plus"
                             label="New token"
                             @click="createOpen = true" />
                </template>
            </DataTable>

            <DataTable v-else
                       :columns="appColumns"
                       :data="apps"
                       :loading="appsLoading"
                       :error="appsError"
                       :row-actions="appActions"
                       no-actions
                       no-row-click
                       search-placeholder="Search apps…"
                       @retry="loadApps" />

            <SettingsTokenCreateModal v-model:open="createOpen"
                                      @created="loadTokens" />
        </template>
    </UDashboardPanel>
</template>
