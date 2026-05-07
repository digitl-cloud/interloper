<script setup lang="ts">
definePageMeta({
    title: 'Welcome',
    layout: false,
})

const organisationStore = useOrganisationStore()
const toast = useToast()

const userStore = useUserStore()
const userName = userStore.user?.name
const name = ref(userName ? `${userName}'s Organisation` : '')
const loading = ref(false)

async function create() {
    if (!name.value.trim())
        return

    loading.value = true
    try {
        await organisationStore.createOrganisation(name.value.trim())
        toast.add({ title: 'Organisation created', color: 'success' })
        await navigateTo('/')
    }
    catch {
        toast.add({ title: 'Failed to create organisation', color: 'error' })
    }
    finally {
        loading.value = false
    }
}
</script>

<template>
    <div class="flex min-h-svh w-full items-center justify-center p-6 md:p-10">
        <div class="w-full max-w-sm">
            <UCard>
                <template #header>
                    <div class="text-center">
                        <h2 class="text-2xl font-semibold">
                            Welcome to Interloper
                        </h2>
                        <p class="text-sm text-muted mt-3">
                            You don't belong to any organisation yet. Create one to get started.
                        </p>
                    </div>
                </template>

                <form class="flex flex-col gap-4"
                      @submit.prevent="create">
                    <UFormField>
                        <UInput v-model="name"
                                class="w-full"
                                placeholder="My Organisation"
                                autofocus
                                :disabled="loading" />
                    </UFormField>

                    <UButton block
                             type="submit"
                             :disabled="!name.trim() || loading"
                             :loading="loading">
                        Create Organisation
                    </UButton>
                </form>
            </UCard>
        </div>
    </div>
</template>
