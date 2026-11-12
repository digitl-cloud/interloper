<script setup lang="ts">
import type { Organisation } from '~/types/organisation'

definePageMeta({
    layout: false,
})

const route = useRoute()
const { apiFetch } = useApi()
const userStore = useUserStore()
const appConfig = useAppConfig()
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
        await navigateTo(`/${joined.key}`)
    }
    catch (err) {
        status.value = 'error'
        errorMessage.value = errorDetail(err) ?? GENERIC_ERROR
    }
})
</script>

<template>
    <div class="flex min-h-svh w-full items-center justify-center p-6">
        <UCard class="w-full max-w-sm">
            <template #header>
                <div class="flex flex-col items-center gap-3 text-center">
                    <LogoIcon class="h-8 text-primary" />
                    <h1 class="text-2xl font-semibold text-highlighted">
                        Invitation
                    </h1>
                </div>
            </template>

            <div v-if="status === 'loading'"
                 class="flex flex-col items-center gap-3 py-6">
                <UIcon :name="appConfig.ui.icons.loading"
                       class="size-5 animate-spin text-dimmed" />
                <p class="text-sm text-muted">
                    Accepting the invitation…
                </p>
            </div>

            <div v-else
                 class="flex flex-col gap-4">
                <UAlert color="error"
                        :icon="appConfig.ui.icons.error"
                        title="Couldn't accept the invitation"
                        :description="errorMessage" />
                <UButton label="Go home"
                         color="neutral"
                         variant="outline"
                         block
                         to="/" />
            </div>
        </UCard>
    </div>
</template>
