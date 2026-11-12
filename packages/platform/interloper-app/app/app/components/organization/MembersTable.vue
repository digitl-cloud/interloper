<script setup lang="ts">
/**
 * An organisation's members and pending invitations, each with an avatar, a
 * role badge and a membership status; admins get the row actions (remove a
 * member, resend or cancel an invitation).
 */
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { OrgMember } from '~/types/organisation'

const UAvatar = resolveComponent('UAvatar')
const UBadge = resolveComponent('UBadge')

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
const appConfig = useAppConfig()

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
                icon: appConfig.icons.invite,
                onSelect: () => emit('resend-invite', member),
            },
        ],
        [
            {
                label: 'Cancel invitation',
                icon: appConfig.icons.cancel,
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
                const avatar = h(UAvatar, {
                    src: member.avatar_url ?? undefined,
                    alt: member.name ?? member.email,
                    text: getInitials(member.name, member.email),
                })
                const name = member.name
                    ? h('span', { class: 'font-medium text-highlighted' }, member.name)
                    : h('span', { class: 'text-dimmed' }, '—')
                const you = member.id === userStore.user?.id
                    ? h(UBadge, { color: 'neutral', size: 'sm', label: 'You' })
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
                return h(UBadge, { color: roleColor(role), label: role.charAt(0).toUpperCase() + role.slice(1) })
            },
        },
        {
            accessorKey: 'status',
            header: 'Status',
            cell: ({ row }) => {
                const active = row.getValue<string>('status') === 'active'
                return h(UBadge, {
                    color: active ? 'success' : 'warning',
                    label: active ? 'Active' : 'Pending',
                })
            },
        },
    ]

    return base
})
</script>

<template>
    <DataTable :columns="columns"
               :data="members"
               :loading="loading"
               :row-actions="isAdmin ? getRowMenuItems : undefined"
               no-actions
               no-row-click
               search-placeholder="Search members…">
        <template #actions>
            <slot name="actions" />
        </template>
    </DataTable>
</template>
