<script setup lang="ts">
definePageMeta({ title: 'Accept Invitation' })

const route = useRoute()
const { apiFetch } = useApi()
const userStore = useUserStore()
const organisationStore = useOrganisationStore()
const toast = useToast()

const status = ref<'loading' | 'success' | 'error'>('loading')
const errorMessage = ref('')

onMounted(async () => {
    // Auth middleware redirects unauthenticated users to login (with redirect back here).
    // For new users, the callback creates an org-less session and redirects here.
    // For existing users, they're already authenticated and land here directly.
    try {
        const result = await apiFetch<{ status: string }>('/auth/accept-invite', {
            method: 'POST',
            body: { token: route.params.token },
        })

        if (result.status === 'already_member') {
            toast.add({ title: 'You are already a member of this organisation', color: 'info' })
        }
        else {
            toast.add({ title: 'You have joined the organisation', color: 'success' })
        }

        // Reload user and org data to pick up new org from session
        await userStore.fetchMe()
        await organisationStore.loadOrganisation()
        status.value = 'success'
        await navigateTo('/')
    }
    catch (err: any) {
        status.value = 'error'
        errorMessage.value = err?.data?.detail || 'Failed to accept invitation'
    }
})
</script>

<template>
    <div class="flex items-center justify-center min-h-screen">
        <div v-if="status === 'loading'" class="text-center">
            <UIcon name="i-lucide-loader-2" class="size-8 animate-spin text-primary" />
            <p class="mt-4 text-muted">Accepting invitation...</p>
        </div>
        <div v-else-if="status === 'error'" class="text-center space-y-4">
            <UIcon name="i-lucide-circle-x" class="size-12 text-error" />
            <p class="text-lg font-medium">Unable to accept invitation</p>
            <p class="text-muted">{{ errorMessage }}</p>
            <UButton label="Go Home" to="/" />
        </div>
    </div>
</template>
