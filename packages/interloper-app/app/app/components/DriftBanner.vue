<script setup lang="ts">
/**
 * Health banner for catalog drift, shared across the Sources and Catalog
 * pages. Surfaces removable drift (sources/assets whose key is gone from the
 * catalog) and offers a one-click, confirmed cleanup. Self-contained: it reads
 * state via useDrift and performs the cleanup itself.
 */
const componentsStore = useComponentsStore()
const toast = useToast()
const { confirm } = useConfirm()
const { hasDrift, missingSources, partialSources, missingAssetCount } = useDrift()

const cleaningUp = ref(false)

/** One-line summary of removable drift. */
const driftSummary = computed(() => {
    const parts: string[] = []
    const sources = missingSources.value.length
    const assets = missingAssetCount.value
    if (sources) parts.push(`${sources} source${sources > 1 ? 's' : ''}`)
    if (assets) parts.push(`${assets} asset${assets > 1 ? 's' : ''}`)
    return parts.join(' and ')
})

async function handleCleanup() {
    const confirmed = await confirm({
        title: 'Clean up catalog drift',
        description: `This permanently removes ${driftSummary.value} that no longer exist in the catalog. This cannot be undone.`,
        confirmLabel: 'Remove',
        confirmColor: 'error',
        icon: 'i-lucide-triangle-alert',
    })
    if (!confirmed) return

    cleaningUp.value = true
    try {
        // Prune drifted assets from still-valid sources, keeping the live ones.
        for (const source of partialSources.value) {
            const keep = source.children.filter(a => a.status !== 'missing').map(a => a.key)
            await componentsStore.update(source.id, { children: keep })
        }
        // Delete sources whose own key is gone from the catalog (assets cascade).
        const missingIds = missingSources.value.map(s => s.id)
        if (missingIds.length) await componentsStore.remove(missingIds)

        await componentsStore.fetchAll(['source'])
        toast.add({ title: 'Catalog drift cleaned up', color: 'success' })
    }
    catch {
        toast.add({ title: 'Failed to clean up drift', color: 'error' })
    }
    finally {
        cleaningUp.value = false
    }
}
</script>

<template>
    <div v-if="hasDrift"
         class="px-4 pt-4">
        <UAlert color="error"
                variant="subtle"
                icon="i-lucide-unplug"
                title="Catalog drift detected"
                :description="`${driftSummary} no longer exist in the catalog`">
            <template #actions>
                <UButton color="error"
                         variant="solid"
                         size="xs"
                         icon="i-lucide-trash-2"
                         label="Clean up"
                         :loading="cleaningUp"
                         @click="handleCleanup" />
            </template>
        </UAlert>
    </div>
</template>
