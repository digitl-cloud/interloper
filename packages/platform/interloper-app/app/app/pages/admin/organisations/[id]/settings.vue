<script setup lang="ts">
const organisationStore = useOrganisationStore()
const userStore = useUserStore()
const toast = useToast()
const { org, isMember, reload, join, openWorkspace } = useAdminOrganisation()

async function onSaved() {
    await userStore.fetchMe()
    await reload()
}

const deleteConfirmName = ref('')
const deleting = ref(false)
const deleteOpen = ref(false)

async function submitDelete() {
    const target = org.value
    if (!target || deleteConfirmName.value !== target.name) return
    deleting.value = true
    try {
        await organisationStore.deleteOrganisation(target.id, deleteConfirmName.value)
        toast.add({ title: `Organisation "${target.name}" deleted`, color: 'success' })
        await navigateTo('/admin/organisations')
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to delete organisation'))
    }
    finally {
        deleting.value = false
    }
}
</script>

<template>
    <div class="flex flex-col gap-4 sm:gap-6">
        <section class="flex flex-col gap-3">
            <CardHeader title="General"
                        description="How the organisation is named and addressed, and your own access to it." />
            <OrganizationGeneralSettings v-if="org"
                                         :organisation="org"
                                         @saved="onSaved">
                <div class="flex items-center gap-4 px-4 py-4 sm:px-6">
                    <div class="min-w-0 flex-1">
                        <div class="text-sm font-medium text-highlighted">Your membership</div>
                        <div class="mt-0.5 text-sm text-muted">
                            <template v-if="isMember">You are an active member of this organisation.</template>
                            <template v-else>You are not a member of this organisation. As a super-admin you can open its workspace anyway, or join it.</template>
                        </div>
                    </div>
                    <UButton v-if="!isMember"
                             icon="i-lucide-user-plus"
                             label="Join"
                             color="neutral"
                             variant="outline"
                             @click="join" />
                    <UButton v-if="org && !org.deleted_at"
                             icon="i-lucide-log-in"
                             label="Open workspace"
                             color="neutral"
                             variant="outline"
                             @click="openWorkspace" />
                </div>
            </OrganizationGeneralSettings>
        </section>

        <section class="flex flex-col gap-3">
            <CardHeader description="Irreversible actions. Proceed only if you are certain.">
                <template #title>
                    <span class="text-error">Danger zone</span>
                </template>
            </CardHeader>
            <UCard :ui="{ root: 'ring-error/40', body: 'p-0 sm:p-0' }">
                <div class="flex items-center gap-4 px-4 py-4 sm:px-6">
                    <div class="min-w-0 flex-1">
                        <div class="text-sm font-medium text-highlighted">Delete organisation</div>
                        <div class="mt-0.5 text-sm text-muted">
                            Permanently deletes {{ org?.name ?? 'this organisation' }} with all its members,
                            invitations, components and execution history.
                        </div>
                    </div>
                    <UButton label="Delete this organisation"
                             color="error"
                             @click="deleteOpen = true" />
                </div>
            </UCard>
        </section>

        <UModal v-model:open="deleteOpen"
                :title="`Delete ${org?.name ?? 'organisation'}?`"
                description="This permanently deletes the organisation with all its members, invitations, components and execution history. This action cannot be undone.">
            <template #body>
                <UFormField :label="`Type “${org?.name}” to confirm`">
                    <UInput v-model="deleteConfirmName"
                            :placeholder="org?.name"
                            class="w-full"
                            @keydown.enter="submitDelete" />
                </UFormField>
            </template>
            <template #footer>
                <div class="flex w-full justify-end gap-2">
                    <UButton label="Cancel"
                             color="neutral"
                             variant="outline"
                             @click="deleteOpen = false" />
                    <UButton label="Delete organisation"
                             color="error"
                             :disabled="deleteConfirmName !== org?.name || deleting"
                             :loading="deleting"
                             @click="submitDelete" />
                </div>
            </template>
        </UModal>
    </div>
</template>
