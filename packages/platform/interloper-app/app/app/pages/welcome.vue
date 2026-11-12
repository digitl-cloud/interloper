<script setup lang="ts">
definePageMeta({
    layout: false,
})

const organisationStore = useOrganisationStore()
const toast = useToast()

const userStore = useUserStore()
const userName = userStore.user?.name
const state = reactive({ name: userName ? `${userName}'s organisation` : '' })
const loading = ref(false)

async function create() {
    if (!state.name.trim())
        return

    loading.value = true
    try {
        const created = await organisationStore.createOrganisation(state.name.trim())
        toast.add({ title: 'Organisation created', color: 'success' })
        await navigateTo(`/${created.key}`)
    }
    catch (e) {
        toast.add(errorToast(e, "Couldn't create the organisation"))
    }
    finally {
        loading.value = false
    }
}
</script>

<template>
    <div class="flex min-h-svh w-full items-center justify-center p-6">
        <UCard class="w-full max-w-sm">
            <template #header>
                <div class="flex flex-col items-center gap-3 text-center">
                    <LogoIcon class="h-8 text-primary" />
                    <div>
                        <h1 class="text-2xl font-semibold text-highlighted">
                            Welcome to Interloper
                        </h1>
                        <p class="mt-1 text-sm text-muted">
                            You don't belong to any organisation yet. Create one to get started.
                        </p>
                    </div>
                </div>
            </template>

            <UForm :state="state"
                   class="flex flex-col gap-4"
                   @submit="create">
                <UFormField label="Organisation name"
                            name="name"
                            required>
                    <UInput v-model="state.name"
                            class="w-full"
                            autofocus
                            :disabled="loading" />
                </UFormField>

                <UButton block
                         type="submit"
                         label="Create organisation"
                         :disabled="!state.name.trim() || loading"
                         :loading="loading" />
            </UForm>
        </UCard>
    </div>
</template>
