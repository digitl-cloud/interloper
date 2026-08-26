<script setup lang="ts">
definePageMeta({ title: 'Profile', layout: 'settings' })

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

const timezones = Intl.supportedValuesOf('timeZone')

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
    <div class="mx-auto w-full max-w-[720px]">
        <div class="mb-3 flex items-center gap-2">
            <UIcon name="i-lucide-user"
                   class="size-4 text-muted" />
            <div class="text-[15px] font-semibold text-highlighted">Account</div>
        </div>

        <div class="divide-y divide-default rounded-lg border border-default bg-elevated/25">
            <div class="flex flex-wrap items-center gap-x-3 gap-y-2.5 px-4 py-3.5">
                <div class="min-w-0 flex-1 basis-60">
                    <div class="text-sm font-medium text-highlighted">User ID</div>
                </div>
                <UInput :model-value="userStore.user?.id"
                        readonly
                        class="min-w-0 flex-1 basis-60 max-w-[280px]"
                        :ui="{ base: 'font-mono text-[13px] text-muted' }" />
            </div>

            <div class="flex flex-wrap items-start gap-x-3 gap-y-2.5 px-4 py-3.5">
                <div class="min-w-0 flex-1 basis-60">
                    <div class="text-sm font-medium text-highlighted">Name</div>
                    <div class="mt-0.5 text-[13px] leading-normal text-muted">
                        Shown to other members of your organisations.
                    </div>
                </div>
                <UInput v-model="name"
                        placeholder="Your name"
                        class="min-w-0 flex-1 basis-60 max-w-[280px]" />
            </div>

            <div class="flex flex-wrap items-start gap-x-3 gap-y-2.5 px-4 py-3.5">
                <div class="min-w-0 flex-1 basis-60">
                    <div class="text-sm font-medium text-highlighted">Email</div>
                    <div class="mt-0.5 text-[13px] leading-normal text-muted">
                        Used for sign-in and notifications. Managed by your Google account.
                    </div>
                </div>
                <UInput :model-value="userStore.user?.email"
                        readonly
                        class="min-w-0 flex-1 basis-60 max-w-[280px]"
                        :ui="{ base: 'text-muted' }" />
            </div>
        </div>

        <div class="mb-3 mt-8 flex items-center gap-2">
            <UIcon name="i-lucide-clock"
                   class="size-4 text-muted" />
            <div class="min-w-0">
                <div class="text-[15px] font-semibold text-highlighted">Time settings</div>
                <div class="mt-0.5 text-[13.5px] leading-normal text-muted">Set your local time zone.</div>
            </div>
        </div>

        <div class="rounded-lg border border-default bg-elevated/25">
            <div class="flex flex-wrap items-start gap-x-3 gap-y-2.5 px-4 py-3.5">
                <div class="min-w-0 flex-1 basis-60">
                    <div class="text-sm font-medium text-highlighted">Time zone</div>
                    <div class="mt-0.5 text-[13px] leading-normal text-muted">
                        Used to display run times and schedules.
                    </div>
                </div>
                <USelectMenu v-model="timezone"
                             :items="timezones"
                             class="min-w-0 flex-1 basis-60 max-w-[280px]" />
            </div>
        </div>

        <div class="mt-6 flex items-center gap-3">
            <div class="min-w-0 flex-1 text-[13px] text-muted">{{ statusText }}</div>
            <UButton label="Save changes"
                     :disabled="!canSave"
                     :loading="saving"
                     @click="save" />
        </div>
    </div>
</template>
