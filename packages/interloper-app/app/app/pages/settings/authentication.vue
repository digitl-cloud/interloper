<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, TabsItem, DropdownMenuItem } from '@nuxt/ui'
import type { PersonalAccessToken } from '~/types/token'

definePageMeta({ title: 'Authentication', layout: 'settings' })

const UBadge = resolveComponent('UBadge')

const route = useRoute()
const router = useRouter()
const userStore = useUserStore()
const toast = useToast()
const { confirm } = useConfirm()
const { apiFetch } = useApi()

const tokens = ref<PersonalAccessToken[]>([])
const loading = ref(false)
const createOpen = ref(false)

const activeTab = computed({
    get: () => (route.query.tab as string) || 'signin',
    set: (value: string) => router.push({ query: { ...route.query, tab: value } }),
})

onMounted(() => {
    if (!route.query.tab) {
        router.replace({ query: { tab: 'signin' } })
    }
    loadTokens()
})

const items = computed<TabsItem[]>(() => [
    { label: 'Sign in', value: 'signin', icon: 'i-lucide-log-in', slot: 'signin' },
    {
        label: 'Personal Access Tokens',
        value: 'tokens',
        icon: 'i-lucide-key',
        slot: 'tokens',
        badge: loading.value ? undefined : tokens.value.length,
    },
])

async function loadTokens() {
    loading.value = true
    try {
        tokens.value = await apiFetch<PersonalAccessToken[]>('/tokens')
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
    <div class="flex flex-col flex-1 min-h-0">
        <UTabs :items="items"
               variant="link"
               :model-value="activeTab"
               @update:model-value="activeTab = $event as string">
            <template #signin>
                <div class="mx-auto w-full max-w-[720px] pt-4">
                    <div class="mb-3 flex items-center gap-2">
                        <UIcon name="i-lucide-log-in"
                               class="size-4 text-muted" />
                        <div class="text-[15px] font-semibold text-highlighted">Sign in methods</div>
                    </div>
                    <div class="rounded-lg border border-default bg-elevated/25">
                        <div class="flex items-center gap-3 px-4 py-3.5">
                            <div class="flex size-7 shrink-0 items-center justify-center rounded-md bg-elevated">
                                <UIcon name="i-devicon-google"
                                       class="size-3.5" />
                            </div>
                            <div class="min-w-0 flex-1">
                                <div class="text-sm font-medium text-highlighted">Google</div>
                                <div class="mt-0.5 text-[13px] text-muted">{{ userStore.user?.email }}</div>
                            </div>
                            <UBadge label="Connected"
                                    color="success"
                                    variant="subtle"
                                    class="shrink-0" />
                        </div>
                    </div>
                </div>
            </template>

            <template #tokens>
                <div class="pt-4">
                    <DataTable :columns="columns"
                               :data="tokens"
                               :loading="loading"
                               :row-actions="rowActions"
                               bordered
                               no-actions
                               no-row-click
                               search-placeholder="Search tokens...">
                        <template #toolbar>
                            <UButton icon="i-lucide-plus"
                                     label="New token"
                                     @click="createOpen = true" />
                        </template>
                    </DataTable>
                </div>
            </template>
        </UTabs>

        <SettingsTokenCreateModal v-model:open="createOpen"
                                  @created="loadTokens" />
    </div>
</template>
