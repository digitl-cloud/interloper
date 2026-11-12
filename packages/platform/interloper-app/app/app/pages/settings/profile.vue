<script setup lang="ts">
definePageMeta({ layout: 'settings' })

const userStore = useUserStore()
const appConfig = useAppConfig()
const toast = useToast()
const { copy, copied } = useClipboard({ copiedDuring: 2000 })

const browserTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone

const state = reactive({ name: '', timezone: browserTimezone })
const saving = ref(false)

function resetDrafts() {
    state.name = userStore.user?.name ?? ''
    state.timezone = userStore.user?.timezone ?? browserTimezone
}

watch(() => userStore.user, resetDrafts, { immediate: true })

const dirty = computed(() => {
    const baseName = userStore.user?.name ?? ''
    const baseTimezone = userStore.user?.timezone ?? browserTimezone
    return state.name.trim() !== baseName || state.timezone !== baseTimezone
})
const valid = computed(() => state.name.trim().length > 0)
const canSave = computed(() => dirty.value && valid.value && !saving.value)

async function save() {
    if (!canSave.value) return
    saving.value = true
    try {
        await userStore.updateProfile({ name: state.name.trim(), timezone: state.timezone })
        toast.add({ title: 'Profile saved', color: 'success' })
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't save the profile"))
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
            <UForm id="profile"
                   :state="state"
                   class="mx-auto flex w-full max-w-2xl flex-col gap-4"
                   @submit="save">
                <UPageCard variant="naked"
                           orientation="horizontal"
                           title="Profile"
                           description="How other members see you, and how times are shown to you.">
                    <UButton form="profile"
                             type="submit"
                             label="Save changes"
                             class="w-fit lg:ms-auto"
                             :disabled="!canSave"
                             :loading="saving" />
                </UPageCard>

                <UCard :ui="{ body: 'flex flex-col gap-4' }">
                    <UFormField label="Name"
                                name="name"
                                description="Shown to other members of your organisations."
                                :error="dirty && !valid ? 'Enter a name.' : undefined"
                                required
                                orientation="horizontal">
                        <UInput v-model="state.name"
                                autocomplete="name"
                                class="w-60" />
                    </UFormField>

                    <USeparator />

                    <UFormField label="Email"
                                description="Used for sign-in and notifications. Managed by your Google account."
                                orientation="horizontal">
                        <span class="text-muted">{{ userStore.user?.email }}</span>
                    </UFormField>

                    <USeparator />

                    <UFormField label="Time zone"
                                name="timezone"
                                description="Used to display run times and schedules."
                                orientation="horizontal">
                        <TimezoneSelect v-model="state.timezone"
                                        class="w-60" />
                    </UFormField>

                    <USeparator />

                    <UFormField v-if="userStore.user"
                                label="User ID"
                                orientation="horizontal">
                        <div class="flex items-center gap-1">
                            <UTooltip :text="userStore.user.id">
                                <span class="text-muted tabular-nums slashed-zero">{{ shortId(userStore.user.id) }}</span>
                            </UTooltip>
                            <UTooltip :text="copied ? 'Copied' : 'Copy user ID'">
                                <UButton :icon="copied ? appConfig.ui.icons.copyCheck : appConfig.ui.icons.copy"
                                         color="neutral"
                                         variant="ghost"
                                         size="sm"
                                         :aria-label="copied ? 'Copied' : 'Copy user ID'"
                                         @click="copy(userStore.user.id)" />
                            </UTooltip>
                        </div>
                    </UFormField>
                </UCard>
            </UForm>
        </template>
    </UDashboardPanel>
</template>
