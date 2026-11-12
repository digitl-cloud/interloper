<script setup lang="ts">
definePageMeta({
    layout: false,
})

const route = useRoute()
const userStore = useUserStore()
const appConfig = useAppConfig()
const loading = ref(false)

const errorMessages: Record<string, string> = {
    signup_not_allowed: 'This Google account is not allowed to sign up. Ask an administrator for an invitation.',
    login_expired: 'The sign-in did not complete in time. Please try again.',
    login_failed: 'Google sign-in did not complete. Please try again.',
}
const error = ref(errorMessages[route.query.error as string] ?? '')

function signInWithGoogle() {
    loading.value = true
    error.value = ''
    const redirect = route.query.redirect as string | undefined
    userStore.signIn(redirect)
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

                <UButton block
                         :icon="appConfig.icons.google"
                         label="Sign in with Google"
                         :loading="loading"
                         @click="signInWithGoogle" />
            </div>
        </UCard>
    </div>
</template>
