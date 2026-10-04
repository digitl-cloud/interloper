<script setup lang="ts">
import type { Organisation } from '~/types/organisation'

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
        const joined = await apiFetch<Organisation>('/auth/accept-invite', {
            method: 'POST',
            body: { token: route.params.token },
        })
        toast.add({ title: `You have joined ${joined.name}`, color: 'success' })

        // Reload user and org data to pick up new org from session
        await userStore.fetchMe()
        await organisationStore.loadOrganisation()
        status.value = 'success'
        await navigateTo('/')
    }
    catch (err) {
        status.value = 'error'
        errorMessage.value = errorDetail(err) ?? 'Failed to accept invitation'
    }
})
</script>

<template>
    <UDashboardPanel id="invite">
        <template #body>
            <div class="flex flex-1 items-center justify-center">
                <div v-if="status === 'loading'"
                     class="text-center">
                    <UIcon name="i-lucide-loader-2"
                           class="size-8 animate-spin text-primary" />
                    <p class="mt-4 text-muted">
                        Accepting invitation...
                    </p>
                </div>
                <div v-else-if="status === 'error'"
                     class="text-center space-y-4">
                    <UIcon name="i-lucide-circle-x"
                           class="size-12 text-error" />
                    <p class="text-lg font-medium">
                        Unable to accept invitation
                    </p>
                    <p class="text-muted">
                        {{ errorMessage }}
                    </p>
                    <UButton label="Go Home"
                             to="/" />
                </div>
            </div>
        </template>
    </UDashboardPanel>
</template>
