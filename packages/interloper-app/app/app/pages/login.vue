<script setup lang="ts">
definePageMeta({
    title: 'Login',
    layout: false,
})

const route = useRoute()
const userStore = useUserStore()
const loading = ref(false)
const error = ref('')

function signInWithGoogle() {
    loading.value = true
    error.value = ''
    const redirect = route.query.redirect as string | undefined
    userStore.signIn(redirect)
}
</script>

<template>
    <div class="flex min-h-svh w-full items-center justify-center p-6 md:p-10">
        <div class="w-full max-w-sm">
            <UCard>
                <template #header>
                    <div class="text-center">
                        <h2 class="text-2xl font-semibold">
                            Interloper
                        </h2>
                        <p class="text-sm text-muted mt-1">
                            Sign in to your account to continue
                        </p>
                    </div>
                </template>

                <div class="flex flex-col gap-4">
                    <UAlert v-if="error"
                            color="error"
                            :title="error"
                            icon="i-lucide-alert-circle" />

                    <UButton block
                             :disabled="loading"
                             icon="i-devicon-google"
                             @click="signInWithGoogle">
                        {{ loading ? 'Redirecting...' : 'Login with Google' }}
                    </UButton>
                </div>
            </UCard>
        </div>
    </div>
</template>
