<script setup lang="ts">
import type { OrgMember } from '~/types/organisation'

const organisationStore = useOrganisationStore()
const toast = useToast()
const appConfig = useAppConfig()
const { orgId, members, loading, isMember, reload, join } = useAdminOrganisation()

const inviteOpen = ref(false)

async function removeMember(member: OrgMember) {
    try {
        await organisationStore.removeMember(orgId.value, member.id)
        toast.add({ title: `${member.name || member.email} removed`, color: 'success' })
        await reload()
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't remove the member"))
    }
}

async function cancelInvite(member: OrgMember) {
    try {
        await organisationStore.cancelInvitation(orgId.value, member.id)
        toast.add({ title: `Invitation to ${member.email} cancelled`, color: 'success' })
        await reload()
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't cancel the invitation"))
    }
}

async function resendInvite(member: OrgMember) {
    try {
        await organisationStore.resendInvitation(orgId.value, member.id)
        toast.add({ title: `Invitation resent to ${member.email}`, color: 'success' })
        await reload()
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't resend the invitation"))
    }
}
</script>

<template>
    <div class="flex flex-col gap-4 sm:gap-6">
        <OrganizationMembersTable :members="members"
                                  :loading="loading"
                                  is-admin
                                  @remove-member="removeMember"
                                  @cancel-invite="cancelInvite"
                                  @resend-invite="resendInvite">
            <template #actions>
                <UButton v-if="!loading && !isMember"
                         :icon="appConfig.icons.signIn"
                         label="Join"
                         color="neutral"
                         variant="outline"
                         @click="join" />
                <UButton :icon="appConfig.icons.invite"
                         label="Invite"
                         @click="inviteOpen = true" />
            </template>
        </OrganizationMembersTable>

        <OrganizationInviteModal v-model:open="inviteOpen"
                                 :org-id="orgId"
                                 @invited="reload" />
    </div>
</template>
