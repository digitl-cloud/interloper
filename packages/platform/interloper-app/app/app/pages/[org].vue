<script setup lang="ts">
/**
 * An organisation's pages, under its key. The route is what makes an
 * organisation the active one; before it does, the key must name one the
 * user can act in (unknown, deleted, or not theirs is a not-found, whatever
 * lies beneath it) and the catalog must be loaded.
 */
definePageMeta({
    middleware: [async (to) => {
        const organisation = await useOrganisationStore().resolve(to.params.org as string)
        if (!organisation)
            return abortNavigation({ statusCode: 404, statusMessage: 'Organisation not found', fatal: true })
        const catalogStore = useCatalogStore()
        if (!catalogStore.loaded) await catalogStore.fetchCatalog(organisation.id)
    }],
})
</script>

<template>
    <NuxtPage />
</template>
