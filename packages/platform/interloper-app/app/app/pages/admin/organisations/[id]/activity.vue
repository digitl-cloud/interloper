<script setup lang="ts">
import type { AdminActivityEntry } from '~/types/admin'

const adminStore = useAdminStore()
const appConfig = useAppConfig()
const { orgId } = useAdminOrganisation()

const activity = ref<AdminActivityEntry[]>([])
const loading = ref(false)
const loadError = ref<unknown>(null)

async function loadActivity() {
    loading.value = true
    loadError.value = null
    try {
        activity.value = await adminStore.getOrgActivity(orgId.value)
    }
    catch (err) {
        loadError.value = err
    }
    finally {
        loading.value = false
    }
}

const ACTIVITY_ICONS: Record<string, string> = {
    org_created: appConfig.icons.organisation,
    org_deleted: appConfig.icons.delete,
    member_joined: appConfig.icons.user,
    invitation_sent: appConfig.icons.invite,
    source_added: appConfig.icons.source,
    runs_completed: appConfig.ui.icons.success,
}

watch(orgId, loadActivity, { immediate: true })
</script>

<template>
    <div class="flex flex-col gap-4">
        <UPageCard variant="naked"
                   orientation="horizontal"
                   title="Activity"
                   description="Derived from membership, invitation, quota and run records." />
        <LoadError v-if="loadError"
                   title="Couldn't load the activity"
                   :error="loadError"
                   @retry="loadActivity" />
        <UCard v-else>
            <div v-if="loading && activity.length === 0"
                 class="flex justify-center py-6">
                <UIcon :name="appConfig.ui.icons.loading"
                       class="size-5 animate-spin text-dimmed" />
            </div>
            <UEmpty v-else-if="activity.length === 0"
                    variant="naked"
                    size="sm"
                    title="Nothing recorded yet" />
            <div v-else
                 class="flex flex-col divide-y divide-default">
                <div v-for="entry in activity"
                     :key="entry.kind + entry.when"
                     class="flex items-start gap-3 py-3 first:pt-0 last:pb-0">
                    <UIcon :name="ACTIVITY_ICONS[entry.kind] ?? appConfig.icons.status"
                           class="mt-0.5 size-4 shrink-0 text-muted" />
                    <div class="min-w-0 flex-1">
                        <div class="text-sm">{{ entry.title }}</div>
                        <div class="mt-0.5 text-xs text-dimmed">
                            <template v-if="entry.detail">{{ entry.detail }} · </template><span class="tabular-nums">{{ relativeTime(new Date(entry.when)) }}</span>
                        </div>
                    </div>
                </div>
            </div>
        </UCard>
    </div>
</template>
