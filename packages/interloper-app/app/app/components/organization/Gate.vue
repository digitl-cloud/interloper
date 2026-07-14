<script setup lang="ts">
import type { Organisation } from '~/types/organisation'

/**
 * Gates a resource detail page on the active organisation.
 *
 * The API authorizes detail reads by membership in the resource's org, so a
 * link can load a resource from an org the user belongs to but doesn't have
 * selected. Instead of rendering that content under the wrong org context,
 * this shows an interstitial offering to switch to the owning org (or go back
 * to the list). Non-member resources 404 server-side and render a not-found
 * state here.
 */
const props = defineProps<{
    /** Org that owns the displayed resource; undefined while it loads. */
    orgId?: string | null
    /** Error from fetching the resource, used to detect 404s. */
    error?: unknown
    /** Where "Back" navigates (the resource's list page). */
    backTo: string
    /** Human label for the resource kind, e.g. "run". */
    resourceLabel: string
}>()

const orgStore = useOrganisationStore()

const mismatch = computed(() =>
    !!props.orgId && !!orgStore.organisation && props.orgId !== orgStore.organisation.id,
)
const notFound = computed(() => (props.error as { statusCode?: number } | null)?.statusCode === 404)

const owningOrg = ref<Organisation | null>(null)
watch(mismatch, async (value) => {
    if (!value) return
    const orgs = await orgStore.fetchOrganisations()
    owningOrg.value = orgs.find(o => o.id === props.orgId) ?? null
}, { immediate: true })

const switching = ref(false)
async function switchToOwningOrg() {
    if (!props.orgId) return
    switching.value = true
    try {
        await orgStore.switchOrg(props.orgId)
    }
    finally {
        switching.value = false
    }
}
</script>

<template>
    <div v-if="notFound"
         class="flex flex-col items-center justify-center gap-3 py-24 text-center">
        <UIcon name="i-lucide-search-x"
               class="size-8 text-muted" />
        <p class="text-sm text-muted">
            This {{ resourceLabel }} wasn't found in your organisations.
        </p>
        <UButton :to="backTo"
                 variant="outline"
                 color="neutral"
                 size="sm"
                 label="Back" />
    </div>
    <div v-else-if="mismatch"
         class="flex flex-col items-center justify-center gap-3 py-24 text-center">
        <UIcon name="i-lucide-building-2"
               class="size-8 text-muted" />
        <p class="text-sm text-muted">
            This {{ resourceLabel }} belongs to
            <span class="font-medium text-default">{{ owningOrg?.name ?? 'another organisation' }}</span>.
        </p>
        <div class="flex items-center gap-2">
            <UButton size="sm"
                     :loading="switching"
                     :label="`Switch to ${owningOrg?.name ?? 'that organisation'}`"
                     @click="switchToOwningOrg" />
            <UButton :to="backTo"
                     variant="outline"
                     color="neutral"
                     size="sm"
                     label="Back" />
        </div>
    </div>
    <slot v-else />
</template>
