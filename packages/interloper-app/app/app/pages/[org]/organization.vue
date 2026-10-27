<script setup lang="ts">
import type { OrgMember } from '~/types/organisation'

const route = useRoute()
const organisationStore = useOrganisationStore()
const userStore = useUserStore()
const toast = useToast()

const rows = ref<OrgMember[]>([])
const loading = ref(false)
const inviteOpen = ref(false)

const isAdmin = computed(() => organisationStore.role === 'admin')
const orgId = computed(() => organisationStore.requireOrganisation().id)

async function loadData() {
    loading.value = true
    try {
        rows.value = await organisationStore.fetchMemberRows(orgId.value, isAdmin.value)
    }
    catch (err) {
        console.error('[Organization] Failed to load members', err)
    }
    finally {
        loading.value = false
    }
}

async function removeMember(member: OrgMember) {
    try {
        await organisationStore.removeMember(orgId.value, member.id)
        toast.add({ title: `${member.name || member.email} removed`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to remove member'))
    }
}

async function cancelInvite(member: OrgMember) {
    try {
        await organisationStore.cancelInvitation(orgId.value, member.id)
        toast.add({ title: `Invitation to ${member.email} cancelled`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to cancel invitation'))
    }
}

async function resendInvite(member: OrgMember) {
    try {
        await organisationStore.resendInvitation(orgId.value, member.id)
        toast.add({ title: `Invitation resent to ${member.email}`, color: 'success' })
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to resend invitation'))
    }
}

onMounted(loadData)

// -- Address --------------------------------------------------------------------

const slug = ref(organisationStore.requireOrganisation().slug)
const slugError = ref<string | null>(null)
const savingSlug = ref(false)
const slugChanged = computed(() => slug.value.trim() !== organisationStore.organisation?.slug)
const origin = window.location.host

/** Move the organisation to a new address; its pages, links and bookmarks follow from there. */
async function saveSlug() {
    const next = slug.value.trim()
    if (!next || !slugChanged.value) return
    const previous = organisationStore.requireOrganisation().slug
    savingSlug.value = true
    slugError.value = null
    try {
        const updated = await organisationStore.updateOrganisation(orgId.value, { slug: next })
        toast.add({ title: `Organisation moved to /${updated.slug}`, color: 'success' })
        await navigateTo({ path: route.path.replace(`/${previous}`, `/${updated.slug}`), query: route.query }, { replace: true })
        await userStore.fetchMe()
    }
    catch (err) {
        slugError.value = errorDetail(err) ?? GENERIC_ERROR
    }
    finally {
        savingSlug.value = false
    }
}

watch(slug, () => {
    slugError.value = null
})
</script>

<template>
    <UDashboardPanel id="organization">
        <template #header>
            <AppNavbar title="Organization" />
        </template>
        <template #body>
            <section v-if="isAdmin"
                     class="flex flex-col gap-3">
                <CardHeader title="Address"
                            description="Every page of this organisation lives under its slug. Old links stop working once it changes." />
                <UCard>
                    <form class="flex items-start gap-2"
                          @submit.prevent="saveSlug">
                        <span class="mt-1.5 text-sm text-muted">{{ origin }}/</span>
                        <UFormField :error="slugError ?? undefined"
                                    class="flex-1">
                            <UInput v-model="slug"
                                    class="w-full"
                                    :disabled="savingSlug" />
                        </UFormField>
                        <UButton type="submit"
                                 label="Save"
                                 :disabled="!slug.trim() || !slugChanged || savingSlug"
                                 :loading="savingSlug" />
                    </form>
                </UCard>
            </section>

            <OrganizationMembersTable :members="rows"
                                      :loading="loading"
                                      :is-admin="isAdmin"
                                      @remove-member="removeMember"
                                      @cancel-invite="cancelInvite"
                                      @resend-invite="resendInvite">
                <template v-if="isAdmin" #actions>
                    <UButton icon="i-lucide-user-plus"
                             label="Invite"
                             @click="inviteOpen = true" />
                </template>
            </OrganizationMembersTable>

            <OrganizationInviteModal v-model:open="inviteOpen"
                                     :org-id="orgId"
                                     @invited="loadData" />
        </template>
    </UDashboardPanel>
</template>
