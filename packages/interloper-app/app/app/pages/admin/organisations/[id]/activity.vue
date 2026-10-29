<script setup lang="ts">
import type { AdminActivityEntry } from '~/types/admin'

const adminStore = useAdminStore()
const { orgId } = useAdminOrganisation()

const activity = ref<AdminActivityEntry[]>([])

async function loadActivity() {
    try {
        activity.value = await adminStore.getOrgActivity(orgId.value)
    }
    catch (err) {
        console.error('[Admin] Failed to load activity', err)
    }
}

const ACTIVITY_ICONS: Record<string, string> = {
    org_created: 'i-lucide-building-2',
    org_deleted: 'i-lucide-trash-2',
    member_joined: 'i-lucide-user-plus',
    invitation_sent: 'i-lucide-mail',
    source_added: 'i-lucide-plug',
    runs_completed: 'i-lucide-check-circle',
}

watch(orgId, loadActivity, { immediate: true })
</script>

<template>
    <section class="flex flex-col gap-3">
        <CardHeader title="Activity"
                    description="Derived from membership, invitation, quota and run records" />
        <UCard :ui="{ body: 'p-0 sm:p-0' }">
            <div class="divide-y divide-default">
                <div v-if="activity.length === 0"
                     class="px-4 py-5 text-sm text-muted sm:px-6">
                    Nothing recorded yet.
                </div>
                <div v-for="entry in activity"
                     :key="entry.kind + entry.when"
                     class="flex items-start gap-3 px-4 py-3 sm:px-6">
                    <span class="mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-md bg-elevated text-muted">
                        <UIcon :name="ACTIVITY_ICONS[entry.kind] ?? 'i-lucide-circle'"
                               class="size-3.5" />
                    </span>
                    <div class="min-w-0 flex-1">
                        <div class="text-sm leading-snug">{{ entry.title }}</div>
                        <div class="mt-0.5 text-xs text-dimmed">
                            <template v-if="entry.detail">{{ entry.detail }} · </template>{{ timeSince(new Date(entry.when)) }} ago
                        </div>
                    </div>
                </div>
            </div>
        </UCard>
    </section>
</template>
