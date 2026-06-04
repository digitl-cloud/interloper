# CHANGELOG

<!-- version list -->

## v0.6.0 (2026-06-04)

### Bug Fixes

- Paginate runs table on executions page
  ([`b2abb2c`](https://github.com/digitl-cloud/interloper/commit/b2abb2ca8222b4d1cd0afc0b668cc013e7a69d58))

- Refetch org-scoped stores when switching organisation
  ([`bfac91c`](https://github.com/digitl-cloud/interloper/commit/bfac91cbe4087d5c1f7d45524758414adb56c9e6))

### Code Style

- Tighten admin breadcrumb spacing
  ([`eb878ef`](https://github.com/digitl-cloud/interloper/commit/eb878eff02f7f40a147fd9369b02dda512443ba8))

### Features

- Add breadcrumbs to admin pages
  ([`46837b9`](https://github.com/digitl-cloud/interloper/commit/46837b963f4fe1e55a74f0a15ee3eea1be6963a4))

- Add platform-wide super-admin role
  ([`196fcfa`](https://github.com/digitl-cloud/interloper/commit/196fcfa6bfce308f0cbe9d04b7a5371da2781a1e))

### Refactoring

- Restyle sidebar org & user controls as dropdown menus
  ([`ef29d12`](https://github.com/digitl-cloud/interloper/commit/ef29d123061c116aec3709db2aeebdd7c9430e21))


## v0.5.0 (2026-06-03)

### Chores

- **app**: Remove dead oauth runtimeConfig from nuxt.config.ts
  ([`01ed43f`](https://github.com/digitl-cloud/interloper/commit/01ed43fc8dde23d5aa7d28699e6e7ca511b9e2ec))

- **chart**: Update replicaCounts
  ([`d34dae2`](https://github.com/digitl-cloud/interloper/commit/d34dae20d80396c2ef3e164d6b1d67ac3f30ceb0))

### Features

- Encrypt resource data at rest with SECRETS_ENCRYPTION_KEY
  ([`5b4a402`](https://github.com/digitl-cloud/interloper/commit/5b4a4020877954921b3257238cac606e96a9cc77))

- **api**: Configure connector OAuth providers via settings
  ([`9c498eb`](https://github.com/digitl-cloud/interloper/commit/9c498eb633852b0705245772dee1bb2ff4957814))

- **chart**: Default FORWARDED_ALLOW_IPS on the API
  ([`d2a50ac`](https://github.com/digitl-cloud/interloper/commit/d2a50accaf3e9f50fb3a61aa56d735163852b694))

- **chart**: Surface connector OAuth config and secrets
  ([`b884f32`](https://github.com/digitl-cloud/interloper/commit/b884f327dd4e9b6e15cd559dd578a6f8dd86a9fc))

### Refactoring

- Rename encryption key env var to INTERLOPER_ENCRYPTION_KEY
  ([`ac66df9`](https://github.com/digitl-cloud/interloper/commit/ac66df9dc3d5b5f220f739d1d0f977e12fe2bc13))

- **api**: Read connector OAuth credentials from provider env vars
  ([`5cdd380`](https://github.com/digitl-cloud/interloper/commit/5cdd38051b57e077499d40150ca645cbde7b1c43))

- **chart**: Drop connector OAuth config/secrets surface
  ([`28de507`](https://github.com/digitl-cloud/interloper/commit/28de507d7184825da79230e0a3a0b9bc7bb08f49))


## v0.4.0 (2026-05-29)

### Features

- **chart**: Support serviceAccount.annotations
  ([`d458cc8`](https://github.com/digitl-cloud/interloper/commit/d458cc8b5b7b67548e164cadc1d49a05af6fedfe))


## v0.3.1 (2026-05-28)

### Bug Fixes

- **docker**: Reconcile uv.lock during image build
  ([`1619ec9`](https://github.com/digitl-cloud/interloper/commit/1619ec9fcabc2d7225360b8eeea5eca9658edf02))


## v0.3.0 (2026-05-28)

### Bug Fixes

- **app**: Unblock nuxt typecheck under strict mode
  ([`040af7f`](https://github.com/digitl-cloud/interloper/commit/040af7fe6589457fc7f708b9508d5cf1b383d81e))

- **frontend**: Use relative redirects so port-forwarded port is preserved
  ([`47bc5f1`](https://github.com/digitl-cloud/interloper/commit/47bc5f1705b66da0573d0ce8a5f33e242493c4c6))

### Chores

- Add /check, /check-py, /build-app slash commands
  ([`476d69c`](https://github.com/digitl-cloud/interloper/commit/476d69c05854eef125471e2d333e6f513a8e2aa9))

- Add shared Claude Code permissions allowlist
  ([`3013e34`](https://github.com/digitl-cloud/interloper/commit/3013e34f0e05e9c595d76bd170e4ff013fca668f))

- Stop adding Claude co-author trailer to commits
  ([`2fe804f`](https://github.com/digitl-cloud/interloper/commit/2fe804ff623b7e8fc6de4867b9911d9e0ea303c6))

- Track all workspace package versions in semantic-release
  ([`8cae464`](https://github.com/digitl-cloud/interloper/commit/8cae464e7a69f832a7795a7cf96ba3b98fe94784))

### Continuous Integration

- Add semantic-release publish workflow
  ([`e5747b0`](https://github.com/digitl-cloud/interloper/commit/e5747b0c54f4c880d59623b8c9574c6d11e62fef))

- Keep semantic-release on 0.x for breaking changes
  ([`367cf45`](https://github.com/digitl-cloud/interloper/commit/367cf45dd26105782c6fdfeb967c28df52d1afab))

- Publish all workspace packages to PyPI
  ([`ebdea1e`](https://github.com/digitl-cloud/interloper/commit/ebdea1e1bfe441b212f97f92a20e5ee99677cbfb))

- Publish docker images to ghcr and helm chart to pages
  ([`f05e5e1`](https://github.com/digitl-cloud/interloper/commit/f05e5e13f02291dacae1fd320d23e86fb8ae7fd7))

- Read semantic-release app id from secrets
  ([`b3aa6be`](https://github.com/digitl-cloud/interloper/commit/b3aa6be81a800af45fe0c4c259b2b029f905cff0))

- Set codecov slug on coverage upload
  ([`96f7853`](https://github.com/digitl-cloud/interloper/commit/96f7853031171ad13da6a4c63e1a17094d8aad5a))

### Documentation

- Add RELEASING guide with PyPI trusted-publishing bootstrap
  ([`e46e9b1`](https://github.com/digitl-cloud/interloper/commit/e46e9b183b715a895ac48abe0c06bfb9013e9302))

- Expand AGENTS.md with workspace map, conventions, and git flow
  ([`e63d4ce`](https://github.com/digitl-cloud/interloper/commit/e63d4ce2948dce4ba6c584aec5b5a614de677d32))

- Point README badges at the interloper repo
  ([`c2aa8e6`](https://github.com/digitl-cloud/interloper/commit/c2aa8e6737c309eee36c068fcf31e8969ce887e6))

- Sync README package table with current workspace
  ([`df12179`](https://github.com/digitl-cloud/interloper/commit/df12179d2ae6249439bec71f6a354fe316401770))

### Features

- **chart**: Add interloper-worker image for k8s runner per-asset jobs
  ([`8e0a9d3`](https://github.com/digitl-cloud/interloper/commit/8e0a9d34b52f55f971d6a35e0bc08e3413dd4880))

### Performance Improvements

- **docker**: Drop dev deps and asset SDKs from api image
  ([`4d0eed1`](https://github.com/digitl-cloud/interloper/commit/4d0eed1b6ba2abd471da77555b2f075ec499037c))

### Refactoring

- Rename images to interloper-<name>:<version>, merge worker into scheduler, shrink images
  ([`c204a40`](https://github.com/digitl-cloud/interloper/commit/c204a40f3ce7e0b530b7d0ff06afeedcb7bfda68))

- **images**: Move launcher suffix from tag to image name
  ([`8545864`](https://github.com/digitl-cloud/interloper/commit/854586446a2fdc0789e5187f1528988f90ab2cac))


## v0.2.0 (2026-05-11)

- Initial Release
