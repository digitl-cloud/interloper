<script setup lang="ts">
import type { LoginProvider } from '~/types/user'

definePageMeta({
    layout: false,
})

const route = useRoute()
const userStore = useUserStore()
const appConfig = useAppConfig()
const { apiFetch } = useApi()

const errorMessages: Record<string, string> = {
    signup_not_allowed: 'This account is not allowed to sign up. Ask an administrator for an invitation.',
    login_expired: 'The sign-in did not complete in time. Please try again.',
    login_failed: 'Sign-in did not complete. Please try again.',
}
const error = ref(errorMessages[route.query.error as string] ?? '')
const providers = ref<LoginProvider[] | null>(null)
const signingIn = ref<string | null>(null)

onMounted(async () => {
    try {
        providers.value = await apiFetch<LoginProvider[]>('/auth/providers')
    }
    catch {
        providers.value = []
        error.value = 'The sign-in options could not be loaded. Please try again.'
    }
})

/** The provider's brand mark from the icon map, or the generic sign-in icon. */
function providerIcon(provider: LoginProvider): string {
    return (appConfig.icons as Record<string, string>)[provider.name] ?? appConfig.icons.signIn
}

function signIn(provider: LoginProvider) {
    signingIn.value = provider.name
    error.value = ''
    userStore.signIn(provider.name, route.query.redirect as string | undefined)
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
                            Interloper
                        </h1>
                        <p class="mt-1 text-sm text-muted">
                            Sign in to your account to continue
                        </p>
                    </div>
                </div>
            </template>

            <div class="flex flex-col gap-4">
                <UAlert v-if="error"
                        color="error"
                        :icon="appConfig.ui.icons.error"
                        :title="error" />

                <USkeleton v-if="providers === null"
                           class="h-8 w-full" />

                <UAlert v-else-if="!providers.length && !error"
                        color="warning"
                        :icon="appConfig.ui.icons.warning"
                        title="No way to sign in is configured. Ask an administrator." />

                <UButton v-for="provider in providers ?? []"
                         :key="provider.name"
                         block
                         :icon="providerIcon(provider)"
                         :label="`Sign in with ${provider.label}`"
                         :loading="signingIn === provider.name"
                         :disabled="signingIn !== null && signingIn !== provider.name"
                         @click="signIn(provider)" />
            </div>
        </UCard>
    </div>
</template>
