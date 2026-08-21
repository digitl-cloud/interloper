<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { OrgMember } from '~/types/organisation'

const UAvatar = resolveComponent('UAvatar')
const UButton = resolveComponent('UButton')
const UDropdownMenu = resolveComponent('UDropdownMenu')
const StatusPill = resolveComponent('StatusPill')

const props = defineProps<{
    members: OrgMember[]
    loading?: boolean
    isAdmin?: boolean
}>()

const emit = defineEmits<{
    'remove-member': [member: OrgMember]
    'cancel-invite': [member: OrgMember]
    'resend-invite': [member: OrgMember]
}>()

const userStore = useUserStore()

function getRowMenuItems(member: OrgMember): DropdownMenuItem[][] {
    if (member.status === 'active') {
        return [
            [
                {
                    label: 'Remove from organisation',
                    icon: 'i-lucide-user-minus',
                    color: 'error' as const,
                    onSelect: () => emit('remove-member', member),
                },
            ],
        ]
    }
    return [
        [
            {
                label: 'Resend invitation',
                icon: 'i-lucide-send',
                onSelect: () => emit('resend-invite', member),
            },
        ],
        [
            {
                label: 'Cancel invitation',
                icon: 'i-lucide-x',
                color: 'error' as const,
                onSelect: () => emit('cancel-invite', member),
            },
        ],
    ]
}

const columns = computed<TableColumn<OrgMember>[]>(() => {
    const base: TableColumn<OrgMember>[] = [
        {
            accessorKey: 'name',
            header: 'Name',
            cell: ({ row }) => {
                const member = row.original
                const avatar = member.avatar_url
                    ? h(UAvatar, { src: member.avatar_url, alt: member.name ?? member.email, size: 'sm' })
                    : h('div', {
                        class: 'size-8 shrink-0 rounded-full flex items-center justify-center text-white text-xs font-semibold',
                        style: { background: avatarColor(member.email || member.id) },
                    }, getInitials(member.name, member.email))
                const name = member.name
                    ? h('span', { class: 'font-semibold text-highlighted' }, member.name)
                    : h('span', { class: 'text-dimmed' }, '—')
                const you = member.id === userStore.user?.id
                    ? h('span', { class: 'px-1.5 py-px rounded-md bg-elevated text-[11px] font-semibold text-dimmed' }, 'You')
                    : null
                return h('div', { class: 'flex items-center gap-3' }, [avatar, name, you])
            },
        },
        {
            accessorKey: 'email',
            header: 'Email',
        },
        {
            accessorKey: 'role',
            header: 'Role',
            cell: ({ row }) => {
                const role = row.getValue<string>('role')
                return h('span', {
                    class: `inline-block px-2.5 py-1 rounded-md text-xs font-semibold ${ROLE_TINTS[role] ?? ROLE_TINTS.viewer}`,
                }, role.charAt(0).toUpperCase() + role.slice(1))
            },
        },
        {
            accessorKey: 'status',
            header: 'Status',
            cell: ({ row }) => {
                const active = row.getValue<string>('status') === 'active'
                return h(StatusPill, {
                    color: active ? 'success' : 'warning',
                    label: active ? 'Active' : 'Pending',
                })
            },
        },
    ]

    if (props.isAdmin) {
        base.push({
            id: 'actions',
            header: '',
            cell: ({ row }) => h('div', { class: 'flex justify-end' }, h(UDropdownMenu, {
                items: getRowMenuItems(row.original),
                content: { align: 'end' },
            }, {
                default: () => h(UButton, {
                    icon: 'i-lucide-ellipsis-vertical',
                    color: 'neutral',
                    variant: 'ghost',
                    size: 'xs',
                    onClick: (e: Event) => e.stopPropagation(),
                }),
            })),
            size: 50,
            enableSorting: false,
            enableGlobalFilter: false,
        })
    }

    return base
})
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <DataTable :columns="columns"
                   :data="members"
                   :loading="loading"
                   :row-actions="isAdmin ? getRowMenuItems : undefined"
                   no-actions
                   no-row-click
                   bordered
                   search-placeholder="Search members...">
            <template #toolbar>
                <slot name="toolbar" />
            </template>
        </DataTable>
    </div>
</template>
