<script setup lang="ts">
import type { Organisation } from '~/types/organisation'

definePageMeta({
    layout: false,
})

const route = useRoute()
const { apiFetch } = useApi()
const userStore = useUserStore()
const toast = useToast()

const status = ref<'loading' | 'error'>('loading')
const errorMessage = ref('')

onMounted(async () => {
    // Auth middleware redirects unauthenticated users to login (with redirect back here).
    try {
        const joined = await apiFetch<Organisation>('/auth/accept-invite', {
            method: 'POST',
            body: { token: route.params.token },
        })
        toast.add({ title: `You have joined ${joined.name}`, color: 'success' })
        await userStore.fetchMe()
        await navigateTo(`/${joined.slug}`)
    }
    catch (err) {
        status.value = 'error'
        errorMessage.value = errorDetail(err) ?? 'Failed to accept invitation'
    }
})
</script>

<template>
    <div class="flex min-h-svh w-full items-center justify-center p-6 md:p-10">
        <div v-if="status === 'loading'"
             class="text-center">
            <UIcon name="i-lucide-loader-2"
                   class="size-8 animate-spin text-primary" />
            <p class="mt-4 text-muted">
                Accepting invitation...
            </p>
        </div>
        <div v-else
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
