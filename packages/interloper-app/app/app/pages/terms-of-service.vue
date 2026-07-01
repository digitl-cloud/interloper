<script setup lang="ts">
definePageMeta({
    title: 'Terms of Service',
    layout: false,
})
</script>

<template>
    <div class="h-svh w-full overflow-y-auto py-12 px-6 md:px-10">
        <article class="mx-auto w-full max-w-3xl flex flex-col gap-6">
            <h1 class="text-3xl font-semibold">
                Terms of Service
            </h1>

            <p>
                These Terms of Service govern your access to and use of Interloper and
                related connector authorization flows provided by DiGiTL Cloud GmbH. By using Interloper,
                connecting a third-party platform, or requesting access tokens through Interloper, you agree
                to these Terms.
            </p>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Scope of the service
                </h2>
                <p>
                    Interloper is provided to help customers authorize supported marketing, analytics,
                    commerce, and social media connectors and to support downstream reporting, data
                    synchronization, and operational workflows requested by the customer. Interloper itself
                    is a facilitation layer for connector access and does not transfer ownership of any
                    third-party account, content, or platform data.
                </p>
            </section>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Eligibility and authority
                </h2>
                <p>
                    You may only use Interloper if you are legally permitted to do so and if you are
                    authorized to act on behalf of the business, brand, or account owner whose connector
                    access you submit. You are responsible for ensuring that any consent you grant through an
                    OAuth flow is accurate, current, and authorized.
                </p>
            </section>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Customer responsibilities
                </h2>
                <p>
                    You are responsible for the accuracy of the credentials, account selections, scopes, and
                    permissions you approve. You must keep your own third-party platform credentials secure,
                    review requested scopes before approving access, and promptly revoke access if you
                    believe a connector has been enabled in error or without proper authorization.
                </p>
            </section>

            <section class="flex flex-col gap-4">
                <h2 class="text-xl font-semibold">
                    Third-party platforms and connector terms
                </h2>
                <p>
                    Your use of any connector is also subject to the terms, developer policies, API rules,
                    and privacy documentation of the applicable third-party platform. We are not responsible
                    for the independent acts, outages, policy changes, interface changes, or enforcement
                    decisions of those third-party providers. If a platform changes its requirements or
                    disables access, we may need to modify, suspend, or remove the relevant connector
                    functionality.
                </p>
                <p>
                    For every platform you connect through Interloper you may only connect an account for
                    which you are authorized to grant the requested scopes; you authorize Interloper to
                    request, receive, process, and store the credentials and data necessary to provide the
                    services you requested; and that authorization is limited to the approved scopes and to
                    your customer relationship with DiGiTL Cloud GmbH. You must not use any connector in a way
                    that violates the relevant platform's rules, infringes third-party rights, misrepresents
                    account ownership, bypasses access controls, or enables unlawful targeting, scraping,
                    resale, or misuse of platform data. The following platform-specific terms apply.
                </p>

                <section class="flex flex-col gap-2">
                    <h3 class="text-lg font-semibold">
                        TikTok
                    </h3>
                    <p>
                        When you connect a TikTok Ads account or TikTok Organic account through Interloper,
                        you acknowledge and agree that your use of those connectors remains subject to the
                        applicable TikTok Terms of Service (<ULink href="https://www.tiktok.com/legal/terms-of-service"
                                                                   target="_blank"
                                                                   rel="noopener noreferrer"
                                                                   class="text-primary">https://www.tiktok.com/legal/terms-of-service</ULink>),
                        developer documentation, API policies, and privacy notices.
                    </p>
                    <p>
                        You may only connect TikTok accounts for which you have full permission to grant the
                        requested access scopes. You authorize Interloper to request, receive, process, and
                        store the TikTok access credentials and TikTok account data that are necessary to
                        provide the analytics, reporting, integration, and support services you requested.
                    </p>
                    <p>
                        That authorization is limited to the scopes approved during the OAuth flow and to the
                        services associated with your customer relationship with DiGiTL Cloud GmbH. You must
                        not use the TikTok connectors through Interloper in a way that violates TikTok rules,
                        infringes third-party rights, misrepresents account ownership, bypasses access
                        controls, or enables unlawful targeting, scraping, resale, or misuse of TikTok data.
                    </p>
                    <p>
                        If TikTok requires user disclosures, additional consent, or revocation mechanisms, you
                        remain responsible for satisfying those obligations in your own business operations.
                        If you revoke Interloper access from within TikTok, remove app permissions, or your
                        TikTok authorization expires, the related connector services may stop working until a
                        new authorization is completed.
                    </p>
                </section>

                <section class="flex flex-col gap-2">
                    <h3 class="text-lg font-semibold">
                        Google
                    </h3>
                    <p>
                        Google connectors (Google Ads, Campaign Manager 360, Display &amp; Video 360, and
                        related services) are subject to the
                        <ULink href="https://developers.google.com/terms"
                               target="_blank"
                               rel="noopener noreferrer"
                               class="text-primary">Google APIs Terms of Service</ULink>.
                        Interloper's use and transfer of information received from Google APIs to any other
                        app adheres to the
                        <ULink href="https://developers.google.com/terms/api-services-user-data-policy"
                               target="_blank"
                               rel="noopener noreferrer"
                               class="text-primary">Google API Services User Data Policy</ULink>,
                        including the Limited Use requirements.
                    </p>
                </section>

                <section class="flex flex-col gap-2">
                    <h3 class="text-lg font-semibold">
                        Meta (Facebook and Instagram)
                    </h3>
                    <p>
                        Meta connectors are subject to the
                        <ULink href="https://developers.facebook.com/terms/"
                               target="_blank"
                               rel="noopener noreferrer"
                               class="text-primary">Meta Platform Terms</ULink>
                        and Meta's developer policies.
                    </p>
                </section>

                <section class="flex flex-col gap-2">
                    <h3 class="text-lg font-semibold">
                        LinkedIn
                    </h3>
                    <p>
                        LinkedIn connectors (LinkedIn Ads and LinkedIn Organic) are subject to the
                        <ULink href="https://legal.linkedin.com/api-terms-of-use"
                               target="_blank"
                               rel="noopener noreferrer"
                               class="text-primary">LinkedIn API Terms of Use</ULink>.
                    </p>
                </section>

                <section class="flex flex-col gap-2">
                    <h3 class="text-lg font-semibold">
                        Amazon
                    </h3>
                    <p>
                        Amazon connectors (Amazon Ads and Amazon Selling Partner) are subject to Amazon's API
                        license agreement, Acceptable Use Policy, and Data Protection Policy.
                    </p>
                </section>

                <section class="flex flex-col gap-2">
                    <h3 class="text-lg font-semibold">
                        Microsoft Advertising (Bing Ads)
                    </h3>
                    <p>
                        The Bing Ads connector is subject to the Microsoft Advertising Agreement and
                        Microsoft's developer policies.
                    </p>
                </section>

                <section class="flex flex-col gap-2">
                    <h3 class="text-lg font-semibold">
                        Pinterest
                    </h3>
                    <p>
                        The Pinterest Ads connector is subject to the
                        <ULink href="https://business.pinterest.com/business-terms-of-service/"
                               target="_blank"
                               rel="noopener noreferrer"
                               class="text-primary">Pinterest Business Terms of Service</ULink>
                        and developer guidelines.
                    </p>
                </section>

                <section class="flex flex-col gap-2">
                    <h3 class="text-lg font-semibold">
                        Snapchat
                    </h3>
                    <p>
                        The Snapchat Ads connector is subject to the Snap Business and developer terms.
                    </p>
                </section>

                <section class="flex flex-col gap-2">
                    <h3 class="text-lg font-semibold">
                        Criteo
                    </h3>
                    <p>
                        The Criteo connector is subject to Criteo's applicable API and platform terms.
                    </p>
                </section>

                <p>
                    Additional connectors may be supported from time to time and are governed by these same
                    terms and by the applicable platform's terms.
                </p>
            </section>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Data use and instructions
                </h2>
                <p>
                    We process connector data only to the extent needed to provide the requested services,
                    maintain connector functionality, troubleshoot issues, meet legal obligations, and
                    protect the service against abuse. You instruct us to process the data made available
                    through the connectors for these purposes when you authorize the connection.
                </p>
            </section>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Acceptable use
                </h2>
                <p>
                    You may not use Interloper to gain unauthorized access, interfere with service
                    availability, circumvent platform restrictions, upload malicious content, violate
                    applicable law, or process data you do not have the right to access. You may not attempt
                    to reverse engineer, disrupt, or misuse Interloper, any connector flow, or any associated
                    infrastructure.
                </p>
            </section>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Availability and changes
                </h2>
                <p>
                    We may update, improve, restrict, or discontinue parts of Interloper at any time,
                    including supported connectors, supported scopes, interface behavior, or security
                    controls. We do not guarantee uninterrupted availability or continued support for any
                    specific third-party integration.
                </p>
            </section>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Intellectual property
                </h2>
                <p>
                    Interloper, its design, and its underlying software remain the property of DiGiTL Cloud
                    GmbH or its licensors, except for third-party marks, content, and materials that remain
                    the property of their respective owners. These Terms do not transfer any intellectual
                    property rights to you other than the limited right to use Interloper for its intended
                    business purpose.
                </p>
            </section>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Disclaimer and limitation of liability
                </h2>
                <p>
                    Interloper is provided on an as-available basis. To the maximum extent permitted by law,
                    DiGiTL Cloud GmbH disclaims warranties not expressly stated, including implied warranties
                    of availability, fitness for a particular purpose, and non-infringement. We are not liable
                    for losses caused by third-party platform downtime, revoked permissions, policy changes,
                    incorrect customer configuration, or unauthorized use of third-party accounts outside our
                    control.
                </p>
            </section>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Termination
                </h2>
                <p>
                    We may restrict or terminate access to Interloper if necessary to comply with law,
                    enforce these Terms, respond to a third-party platform requirement, address security
                    concerns, or prevent abuse. You may stop using Interloper at any time and may revoke
                    connector authorizations through the relevant third-party platform or your Interloper
                    service contacts where applicable.
                </p>
            </section>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Governing law and contact
                </h2>
                <p>
                    These Terms are governed by the laws of Germany, excluding conflict-of-law rules, unless
                    mandatory law requires otherwise. Questions regarding these Terms may be sent to DiGiTL
                    Cloud GmbH, An der Alster 6, 20099 Hamburg, Germany, or by email to
                    <ULink href="mailto:info@digitlcloud.com" class="text-primary">
                        info@digitlcloud.com
                    </ULink>.
                </p>
            </section>

            <section class="flex flex-col gap-2">
                <h2 class="text-xl font-semibold">
                    Effective date
                </h2>
                <p>
                    These Terms of Service apply as of 01 January 2026.
                </p>
            </section>
        </article>
    </div>
</template>
