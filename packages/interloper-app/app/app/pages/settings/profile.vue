<script setup lang="ts">
definePageMeta({ layout: 'settings' })

const userStore = useUserStore()
const toast = useToast()

const browserTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone

const name = ref('')
const timezone = ref(browserTimezone)
const saved = ref(false)
const saving = ref(false)

function resetDrafts() {
    name.value = userStore.user?.name ?? ''
    timezone.value = userStore.user?.timezone ?? browserTimezone
}

watch(() => userStore.user, resetDrafts, { immediate: true })
watch([name, timezone], () => {
    saved.value = false
})

const dirty = computed(() => {
    const baseName = userStore.user?.name ?? ''
    const baseTimezone = userStore.user?.timezone ?? browserTimezone
    return name.value.trim() !== baseName || timezone.value !== baseTimezone
})
const valid = computed(() => name.value.trim().length > 0)
const canSave = computed(() => dirty.value && valid.value && !saving.value)

const statusText = computed(() => {
    if (saved.value) return 'Changes saved.'
    if (dirty.value && !valid.value) return 'Enter a name.'
    if (dirty.value) return 'Unsaved changes.'
    return ''
})

async function save() {
    if (!canSave.value) return
    saving.value = true
    try {
        await userStore.updateProfile({ name: name.value.trim(), timezone: timezone.value })
        saved.value = true
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to save profile'))
    }
    finally {
        saving.value = false
    }
}
</script>

<template>
    <UDashboardPanel id="profile">
        <template #header>
            <AppNavbar title="Profile" />
        </template>
        <template #body>
            <div class="mx-auto flex w-full max-w-3xl flex-col gap-4 sm:gap-6">
                <section class="flex flex-col gap-3">
                    <CardHeader title="Account" />
                    <UCard :ui="{ body: 'p-0 sm:p-0' }">
                        <div class="divide-y divide-default">
                            <div class="flex flex-wrap items-center gap-x-3 gap-y-2.5 px-4 py-4 sm:px-6">
                                <div class="min-w-0 flex-1 basis-60">
                                    <div class="text-sm font-medium text-highlighted">User ID</div>
                                </div>
                                <UInput :model-value="userStore.user?.id"
                                        readonly
                                        class="min-w-0 max-w-70 flex-1 basis-60"
                                        :ui="{ base: 'font-mono text-sm text-muted' }" />
                            </div>

                            <div class="flex flex-wrap items-start gap-x-3 gap-y-2.5 px-4 py-4 sm:px-6">
                                <div class="min-w-0 flex-1 basis-60">
                                    <div class="text-sm font-medium text-highlighted">Name</div>
                                    <div class="mt-0.5 text-sm leading-normal text-muted">
                                        Shown to other members of your organisations.
                                    </div>
                                </div>
                                <UInput v-model="name"
                                        placeholder="Your name"
                                        class="min-w-0 max-w-70 flex-1 basis-60" />
                            </div>

                            <div class="flex flex-wrap items-start gap-x-3 gap-y-2.5 px-4 py-4 sm:px-6">
                                <div class="min-w-0 flex-1 basis-60">
                                    <div class="text-sm font-medium text-highlighted">Email</div>
                                    <div class="mt-0.5 text-sm leading-normal text-muted">
                                        Used for sign-in and notifications. Managed by your Google account.
                                    </div>
                                </div>
                                <UInput :model-value="userStore.user?.email"
                                        readonly
                                        class="min-w-0 max-w-70 flex-1 basis-60"
                                        :ui="{ base: 'text-muted' }" />
                            </div>
                        </div>
                    </UCard>
                </section>

                <section class="flex flex-col gap-3">
                    <CardHeader title="Time settings"
                                description="Set your local time zone." />
                    <UCard :ui="{ body: 'p-0 sm:p-0' }">
                        <div class="divide-y divide-default">
                            <div class="flex flex-wrap items-start gap-x-3 gap-y-2.5 px-4 py-4 sm:px-6">
                                <div class="min-w-0 flex-1 basis-60">
                                    <div class="text-sm font-medium text-highlighted">Time zone</div>
                                    <div class="mt-0.5 text-sm leading-normal text-muted">
                                        Used to display run times and schedules.
                                    </div>
                                </div>
                                <TimezoneSelect v-model="timezone"
                                                class="min-w-0 max-w-70 flex-1 basis-60" />
                            </div>
                        </div>
                    </UCard>
                </section>

                <div class="flex items-center gap-3">
                    <div class="min-w-0 flex-1 text-sm text-muted">{{ statusText }}</div>
                    <UButton label="Save changes"
                             :disabled="!canSave"
                             :loading="saving"
                             @click="save" />
                </div>
            </div>
        </template>
    </UDashboardPanel>
</template>
