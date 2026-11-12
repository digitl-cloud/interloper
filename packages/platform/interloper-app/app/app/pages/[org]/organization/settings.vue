<script setup lang="ts">
import type { Organisation } from '~/types/organisation'

// Only the organisation's admins can change it; anyone else lands on its members.
definePageMeta({
    middleware: [async (to) => {
        const organisation = await useOrganisationStore().resolve(to.params.org as string)
        if (organisation?.role !== 'admin')
            return navigateTo(`/${to.params.org}/organization/members`, { replace: true })
    }],
})

const route = useRoute()
const organisationStore = useOrganisationStore()
const userStore = useUserStore()
const views = useOrganizationViews()

/** Follow a new key to the organisation's new address, then refresh the memberships it is listed under. */
async function onSaved(updated: Organisation) {
    const previous = route.params.org as string
    if (updated.key !== previous)
        await navigateTo({ path: route.path.replace(`/${previous}`, `/${updated.key}`), query: route.query }, { replace: true })
    await userStore.fetchMe()
}
</script>

<template>
    <UDashboardPanel id="organization-settings">
        <template #header>
            <AppNavbar title="Organisation" />
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="views" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <OrganizationGeneralSettings :organisation="organisationStore.requireOrganisation()"
                                         title="General"
                                         description="How the organisation is named and addressed."
                                         class="mx-auto w-full max-w-2xl"
                                         @saved="onSaved" />
        </template>
    </UDashboardPanel>
</template>
