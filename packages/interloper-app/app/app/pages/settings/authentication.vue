<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, NavigationMenuItem, DropdownMenuItem } from '@nuxt/ui'
import type { PersonalAccessToken } from '~/types/token'

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

// Anything but ?tab=tokens, unknown values included, is the sign-in pane.
const activeTab = computed(() => route.query.tab === 'tokens' ? 'tokens' : 'signin')

onMounted(() => {
    if (!route.query.tab) {
        router.replace({ query: { tab: 'signin' } })
    }
    loadTokens()
})

const tabs = computed<NavigationMenuItem[]>(() => [
    { label: 'Sign in', icon: 'i-lucide-log-in', to: { query: { tab: 'signin' } }, active: activeTab.value === 'signin' },
    {
        label: 'Personal Access Tokens',
        icon: 'i-lucide-key',
        badge: loading.value ? undefined : tokens.value.length,
        to: { query: { tab: 'tokens' } },
        active: activeTab.value === 'tokens',
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
                    <UNavigationMenu :items="tabs"
                                     highlight
                                     class="-mx-1 flex-1" />
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

            <DataTable v-else
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

            <SettingsTokenCreateModal v-model:open="createOpen"
                                      @created="loadTokens" />
        </template>
    </UDashboardPanel>
</template>
