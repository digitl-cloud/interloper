<script setup lang="ts">
const organisationStore = useOrganisationStore()
const userStore = useUserStore()
const toast = useToast()
const appConfig = useAppConfig()
const { org, isMember, reload, join } = useAdminOrganisation()

async function onSaved() {
    await userStore.fetchMe()
    await reload()
}

const deleteState = reactive({ name: '' })
const deleting = ref(false)
const deleteOpen = ref(false)

async function submitDelete() {
    const target = org.value
    if (!target || deleteState.name !== target.name) return
    deleting.value = true
    try {
        await organisationStore.deleteOrganisation(target.id, deleteState.name)
        toast.add({ title: `Organisation "${target.name}" deleted`, color: 'success' })
        await navigateTo('/admin/organisations')
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't delete the organisation"))
    }
    finally {
        deleting.value = false
    }
}
</script>

<template>
    <div class="mx-auto flex w-full max-w-2xl flex-col gap-4 sm:gap-6">
        <OrganizationGeneralSettings v-if="org"
                                     :organisation="org"
                                     title="General"
                                     description="How the organisation is named and addressed, and your own access to it."
                                     @saved="onSaved">
            <UFormField label="Your membership"
                        :description="isMember
                            ? 'You are an active member of this organisation.'
                            : 'You are not a member. As a super admin you can open its workspace anyway, or join it.'"
                        orientation="horizontal">
                <UBadge v-if="isMember"
                        color="success"
                        label="Member" />
                <UButton v-else
                         :icon="appConfig.icons.signIn"
                         label="Join"
                         color="neutral"
                         variant="outline"
                         @click="join" />
            </UFormField>
        </OrganizationGeneralSettings>

        <section class="flex flex-col gap-4">
            <UPageCard variant="naked"
                       orientation="horizontal"
                       title="Danger zone"
                       description="Irreversible actions. Proceed only if you are certain." />
            <UCard>
                <UFormField label="Delete organisation"
                            :description="`Permanently deletes ${org?.name ?? 'this organisation'} with all its members, invitations, components and execution history.`"
                            orientation="horizontal">
                    <UButton label="Delete organisation"
                             color="error"
                             variant="soft"
                             @click="deleteOpen = true" />
                </UFormField>
            </UCard>
        </section>

        <UModal v-model:open="deleteOpen"
                :title="`Delete ${org?.name ?? 'organisation'}?`"
                description="This permanently deletes the organisation with all its members, invitations, components and execution history. This action cannot be undone.">
            <template #body>
                <UForm id="delete-organisation"
                       :state="deleteState"
                       @submit="submitDelete">
                    <UFormField :label="`Type “${org?.name}” to confirm`"
                                name="name">
                        <UInput v-model="deleteState.name"
                                :placeholder="org?.name"
                                class="w-full" />
                    </UFormField>
                </UForm>
            </template>
            <template #footer>
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="deleteOpen = false" />
                <UButton label="Delete organisation"
                         type="submit"
                         form="delete-organisation"
                         color="error"
                         :disabled="deleteState.name !== org?.name || deleting"
                         :loading="deleting" />
            </template>
        </UModal>
    </div>
</template>
