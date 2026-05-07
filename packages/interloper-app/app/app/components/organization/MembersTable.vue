<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { OrgMember } from '~/types/organisation'

const UAvatar = resolveComponent('UAvatar')
const UBadge = resolveComponent('UBadge')
const UButton = resolveComponent('UButton')
const UDropdownMenu = resolveComponent('UDropdownMenu')

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

function getInitials(member: OrgMember): string {
    const name = member.name?.trim()
    if (name && name.length > 0) {
        const parts = name.split(/\s+/)
        const first = parts[0]?.[0] ?? ''
        const last = parts.length > 1 ? (parts[parts.length - 1]?.[0] ?? '') : ''
        return last ? (first + last).toUpperCase() : first.toUpperCase()
    }
    return member.email?.charAt(0).toUpperCase() ?? '?'
}

const roleColor: Record<string, 'primary' | 'success' | 'neutral'> = {
    admin: 'primary',
    editor: 'success',
    viewer: 'neutral',
}

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
            id: 'avatar',
            header: '',
            cell: ({ row }) => h(UAvatar, {
                src: row.original.avatar_url ?? undefined,
                text: getInitials(row.original),
                alt: row.original.name ?? row.original.email,
                size: 'sm',
            }),
            size: 50,
            enableSorting: false,
            enableGlobalFilter: false,
        },
        {
            accessorKey: 'name',
            header: 'Name',
            cell: ({ row }) => row.original.name ?? '—',
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
                return h(UBadge, {
                    color: roleColor[role] ?? 'neutral',
                    variant: 'subtle',
                }, () => role.charAt(0).toUpperCase() + role.slice(1))
            },
        },
        {
            accessorKey: 'status',
            header: 'Status',
            cell: ({ row }) => {
                const status = row.getValue<string>('status')
                return h(UBadge, {
                    color: status === 'active' ? 'success' : 'warning',
                    variant: 'subtle',
                }, () => status === 'active' ? 'Active' : 'Invited')
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
                   search-placeholder="Search members...">
            <template #toolbar>
                <slot name="toolbar" />
            </template>
        </DataTable>
    </div>
</template>
