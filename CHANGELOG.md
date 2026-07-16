# CHANGELOG

<!-- version list -->

## v0.39.0 (2026-07-16)

### Bug Fixes

- **app**: Pass the partition date when running a job from the UI
  ([`92040a9`](https://github.com/digitl-cloud/interloper/commit/92040a9ac8e0d3b514bb15b1d9eec035ca8dceea))

### Features

- **app**: Tag filter tabs on the wizard asset list
  ([`728f332`](https://github.com/digitl-cloud/interloper/commit/728f332569c8d83fe3d310f94b24204dd497071d))


## v0.38.0 (2026-07-16)

### Bug Fixes

- Keep declared field order when a field shadows a Serializable ClassVar
  ([`c617cd6`](https://github.com/digitl-cloud/interloper/commit/c617cd6b665375740c732cf5f94648af058d5341))

### Documentation

- Entity assets are now time-partitioned on a stamped date
  ([`cb4fd81`](https://github.com/digitl-cloud/interloper/commit/cb4fd8130aa755e359eb36ae36c70904403a4f7a))

### Features

- **app**: Polish the source wizard asset list
  ([`6d08ab1`](https://github.com/digitl-cloud/interloper/commit/6d08ab134ef31e25157205375f0cc95e9f5988cc))

- **assets**: Time-partition every entity asset
  ([`03813aa`](https://github.com/digitl-cloud/interloper/commit/03813aafe57c49d0df9fe3f0a82f9452f573f364))

### Refactoring

- **assets**: Declare the stamped date first on entity schemas
  ([`bb87ec8`](https://github.com/digitl-cloud/interloper/commit/bb87ec8796599edccf3859eb0cd868b3b4f64883))


## v0.37.0 (2026-07-16)

### Features

- **assets**: Add The Trade Desk source
  ([`d72d968`](https://github.com/digitl-cloud/interloper/commit/d72d968d5629f6b7f8105d2d65f7fbd455d78307))


## v0.36.0 (2026-07-15)

### Features

- **app**: Agent panel UX polish
  ([`ae673d0`](https://github.com/digitl-cloud/interloper/commit/ae673d078f3bae66d951fcacb727115ee0a74278))


## v0.35.0 (2026-07-15)

### Bug Fixes

- **app**: Cap the agent composer at six rows
  ([`b13dd79`](https://github.com/digitl-cloud/interloper/commit/b13dd7911ab73e3638c5053f2c59d50669d86dd3))

- **db**: Stop the ORM orphaning child assets on source deletion
  ([`47d6410`](https://github.com/digitl-cloud/interloper/commit/47d6410540629c94c3a38fec2e5c5336750e744e))

### Chores

- **app**: Update the agent's default suggested prompts
  ([`55caed8`](https://github.com/digitl-cloud/interloper/commit/55caed80772150f8965a78c43cb99b56f4d6ea8a))

### Features

- Relation-aware deletion semantics via on_delete vocabulary
  ([`8877b15`](https://github.com/digitl-cloud/interloper/commit/8877b155e04c9ce71dc21c868b0a33de2394b675))

- **agent**: React-only bulk connection creation from volunteered credentials
  ([`d9720f4`](https://github.com/digitl-cloud/interloper/commit/d9720f41c1dfd71be78f3c56b6d362482ebf243b))

- **app**: Badge the subject entity in delete confirmations
  ([`e073f96`](https://github.com/digitl-cloud/interloper/commit/e073f96eb7d356bc3e316d5582ce6538e70c7891))

- **app**: Preview "used by" in delete confirmations
  ([`f1f7e80`](https://github.com/digitl-cloud/interloper/commit/f1f7e80ed08e52031cb7360416da8dedd4003423))

- **app**: Render delete-dialog referrers as a compact table
  ([`8b297e0`](https://github.com/digitl-cloud/interloper/commit/8b297e0d2f99a4a23381af315ef7b0e1bfc6399d))

- **app**: Unify delete confirmations on the shared ConfirmModal
  ([`4166cc9`](https://github.com/digitl-cloud/interloper/commit/4166cc9c362f106379698bd552c097e0614541ef))

- **assets**: Add Brandwatch source
  ([`16d71ca`](https://github.com/digitl-cloud/interloper/commit/16d71ca92d74e7dc42d5c9e18c3a03d7fc282e7f))

- **assets**: Add Campaign Manager 360 source
  ([`05b3fef`](https://github.com/digitl-cloud/interloper/commit/05b3fefeca2b0a775fcfb760aa5be3fd68f23627))

- **assets**: Add Display & Video 360 source
  ([`64ac1b5`](https://github.com/digitl-cloud/interloper/commit/64ac1b5330185354990cca698f92a1d0e89b592c))

- **assets**: Fold Bid Manager reports into the DV360 source
  ([`488c5e8`](https://github.com/digitl-cloud/interloper/commit/488c5e8e4f21f2bc44e539722a28b30e166bc962))

### Refactoring

- **assets**: Name the DV360 clients by their API
  ([`1f997df`](https://github.com/digitl-cloud/interloper/commit/1f997dfd4bc13e9bde27e5d6b5f5b6cc04c64c86))


## v0.34.0 (2026-07-15)

### Bug Fixes

- **app**: Align agent panel header and composer with the app chrome
  ([`91e490b`](https://github.com/digitl-cloud/interloper/commit/91e490bd7accec2e7caaf1fdbd09d0fa70a55ee7))

- **core**: Detect IPv6-only listeners in the dev-up port fail-fast
  ([`2de136f`](https://github.com/digitl-cloud/interloper/commit/2de136f5dba4e5f5ec351c525866c6d88737203d))

### Features

- **assets**: Add Amazon Selling Partner source
  ([`1c5bc93`](https://github.com/digitl-cloud/interloper/commit/1c5bc93fad635aeb8f793fcc6e2886fa90e3b557))

### Refactoring

- **app**: Drop encrypted column from resources table
  ([`8a10625`](https://github.com/digitl-cloud/interloper/commit/8a106257e8868f18ca4bdbcd8cb3d7b690fcc1a9))


## v0.33.0 (2026-07-14)

### Bug Fixes

- **agent**: Asset selection card, no field guessing, no error bleed
  ([`1eb0df9`](https://github.com/digitl-cloud/interloper/commit/1eb0df9707607c70775071da8ce07498ae79e42f))

- **agent**: Contain google-adk's BaseAgentConfig deprecation at the import site
  ([`995dc72`](https://github.com/digitl-cloud/interloper/commit/995dc72316d237527b864d2a94145b65c5214dbf))

- **agent**: Inject the current time into agent instructions
  ([`26f5537`](https://github.com/digitl-cloud/interloper/commit/26f5537794ff01aa5b19cb45f7dded5d287a4d30))

- **agent**: Never auto-attach a destination — ask, don't reuse silently
  ([`cd58c6d`](https://github.com/digitl-cloud/interloper/commit/cd58c6d27abc5e6b5ea4c816125155320b664e3b))

- **agent**: Scope source listing to the organisation
  ([`ef55226`](https://github.com/digitl-cloud/interloper/commit/ef5522636483a8c5f0d5d8b7b01b2e108b15a4a0))

- **app**: Hide the test button while the OAuth sign-in tab is active
  ([`fe853bd`](https://github.com/digitl-cloud/interloper/commit/fe853bd71500848e9d50ce8dceb8b64e32c0e553))

### Chores

- Allow force-with-lease pushes in agent sessions
  ([`ba39ffe`](https://github.com/digitl-cloud/interloper/commit/ba39ffe860fce77524f676448c7df2245a4636e5))

- **test**: Silence google-adk's self-inflicted BaseAgentConfig warning
  ([`82c6dbf`](https://github.com/digitl-cloud/interloper/commit/82c6dbf1f62b58b00c5f6b07148b9bbf6f792e65))

### Features

- Connection validation with a wizard test step
  ([`288de15`](https://github.com/digitl-cloud/interloper/commit/288de159e141e8cfd7b777917e640f95b11aca9a))

- Discriminator markers for remaining multi-instance components
  ([`f93e47c`](https://github.com/digitl-cloud/interloper/commit/f93e47c7f3bf3abb1e7a41b0d06460f24ebacb62))

- Instance names derive from the discriminator alone
  ([`3d33b4d`](https://github.com/digitl-cloud/interloper/commit/3d33b4d86d08ce418b491538fe62284727f07c70))

- Refuse deleting components that are still in use
  ([`63b317f`](https://github.com/digitl-cloud/interloper/commit/63b317f6c0bb47b84902b0992547b7603d7263ec))

- **agent**: Bulk multi-account sources and the job that schedules them
  ([`3a42e93`](https://github.com/digitl-cloud/interloper/commit/3a42e93c2d24955344e2218cf5e92dbb3fffca1c))

- **agent**: Collection agent consults the catalog specialist as a tool
  ([`0330824`](https://github.com/digitl-cloud/interloper/commit/033082495fdc22edee672391ad9c9e1b5bbdc054))

- **agent**: Confirmation summary card for creations
  ([`12d15dd`](https://github.com/digitl-cloud/interloper/commit/12d15dd62296ec8541d057ba9872c8571202c7bf))

- **agent**: Connection setup through the chat
  ([`8f0a73f`](https://github.com/digitl-cloud/interloper/commit/8f0a73fd4d37fed4b7e694f8fe11f355f4f88a29))

- **agent**: Conversational source setup
  ([`db35a78`](https://github.com/digitl-cloud/interloper/commit/db35a78d0342025a62137296b08fbc065c55f93c))

- **agent**: One source-setup flow — the account selection decides plurality
  ([`7a6fa91`](https://github.com/digitl-cloud/interloper/commit/7a6fa91169d39bee54fc5065187a6aa3df828cc3))

- **agent**: Scheduling agent creates jobs too
  ([`b5d5272`](https://github.com/digitl-cloud/interloper/commit/b5d527292dc344acefebd2826cb449b71b489770))

- **agent**: Shared presentation rules for all sub-agents
  ([`d642b82`](https://github.com/digitl-cloud/interloper/commit/d642b82142958539c26b92d49c221ca78a4f3c21))

- **agent**: The agent validates connections with the check hook
  ([`9fa3f8f`](https://github.com/digitl-cloud/interloper/commit/9fa3f8f75ad59fdbb368c31adebe98bc088e7dd5))

- **app**: Connect card gates creation on the candidate check
  ([`8e10ca5`](https://github.com/digitl-cloud/interloper/commit/8e10ca5d1aa4c8d4a4e50369c23e86d3c4a2079d))

- **google-cloud**: Google Cloud Storage destination
  ([`db5e78c`](https://github.com/digitl-cloud/interloper/commit/db5e78c5bb22d9f5dad78a702cfe2f20c38b05ee))

### Refactoring

- **agent**: Agents split by space, tool names drop the prefixes
  ([`07dc4a2`](https://github.com/digitl-cloud/interloper/commit/07dc4a29921631e239210de615df10a08cd681ea))

- **agent**: Compact COLLECTION_INSTRUCTION, same behavior
  ([`a83004c`](https://github.com/digitl-cloud/interloper/commit/a83004c5325d346bfe9abdb792686eb7593d9516))

- **agent**: Compact the remaining agent instructions, same behavior
  ([`571e0ea`](https://github.com/digitl-cloud/interloper/commit/571e0ea6bf03a56ed8cb49b735938ec0cf704471))

- **agent**: Connection check hydrates through the store
  ([`53e4912`](https://github.com/digitl-cloud/interloper/commit/53e491244bf5a057ed4e70796142f1652361b0a4))

- **agent**: Generic component tools in flat catalog/collection modules
  ([`427937d`](https://github.com/digitl-cloud/interloper/commit/427937d2b1f6de1ddad5f99774129361467b36f9))

- **agent**: Merge actions and operations tools into scheduling
  ([`a247e7a`](https://github.com/digitl-cloud/interloper/commit/a247e7aa63a61fc72dcec926b242b19556474913))

- **agent**: Tools organized into catalog and collection subpackages
  ([`b31671e`](https://github.com/digitl-cloud/interloper/commit/b31671ec0b203283e697b63d98303eb65d8ac72d))

- **agent**: Tools split by component kind, catalog/collection taxonomy
  ([`aff8da9`](https://github.com/digitl-cloud/interloper/commit/aff8da9a658e53c7d39cca433a97e73da8a646a9))

- **api**: Fold resolve and check into routes/components.py
  ([`59a251b`](https://github.com/digitl-cloud/interloper/commit/59a251b33a0c068fedc311a3d26cd826ff4d09ac))

- **api**: Serve resolve and check under /components
  ([`922b955`](https://github.com/digitl-cloud/interloper/commit/922b95534ab833b43f33ab56c36124032dc3051c))

- **app**: Agent cards adopt the wizard's design language, compacted
  ([`81b0741`](https://github.com/digitl-cloud/interloper/commit/81b0741b39de316538bed32447818cf8911d4ba9))

- **app**: Consolidate table column badges
  ([`8f54787`](https://github.com/digitl-cloud/interloper/commit/8f5478736fc02ca59de2a8335b93b63ed1cd307f))

- **app**: Full-width test button directly under the credentials
  ([`5945a21`](https://github.com/digitl-cloud/interloper/commit/5945a21016f2e605e28d2c5ac6e8524fe9818e5f))

- **app**: Namespace root components
  ([`46f2f54`](https://github.com/digitl-cloud/interloper/commit/46f2f54a810ad668e9bd231e8507e6951447872d))

- **app**: Regorganize stepper details step sections
  ([`21a4a32`](https://github.com/digitl-cloud/interloper/commit/21a4a32132ef4b72c7586159b298aa96a490b8e8))

- **app**: Rename the catalog page to collection
  ([`97f3696`](https://github.com/digitl-cloud/interloper/commit/97f369628f0b2cc341f28dce8a2c9d1c123f7398))

- **app**: Surface config errors via Nuxt UI form validation
  ([`9787f54`](https://github.com/digitl-cloud/interloper/commit/9787f545547e816a015bfa1fe34157c87c1113af))

- **app**: Test connection via a button on the details step
  ([`7e9cfbf`](https://github.com/digitl-cloud/interloper/commit/7e9cfbf0088195c1341966c04da865ef0dabe407))

- **oauth**: One owner for the provider env convention
  ([`5e0d47b`](https://github.com/digitl-cloud/interloper/commit/5e0d47b4508ebc405aa6824cbe17ba173735fbf3))


## v0.32.0 (2026-07-10)

### Bug Fixes

- **api**: Warn when an invitation email is skipped because SMTP is unconfigured
  ([`e97d4eb`](https://github.com/digitl-cloud/interloper/commit/e97d4eb5f7498322efed56e31faba8b5dca4afb1))

- **app**: Bump vue-router to v5 to match nuxt 4.4
  ([`bbb5abd`](https://github.com/digitl-cloud/interloper/commit/bbb5abde5733e3445e892a6062f3c92d00bd1e0a))

- **app**: Keep the table empty state stable during background refetches
  ([`53387fd`](https://github.com/digitl-cloud/interloper/commit/53387fdd5de9e67ea8162a67d00a589c1aed7332))

- **core**: DAG.from_spec follows the construction grammar
  ([`df4adb7`](https://github.com/digitl-cloud/interloper/commit/df4adb7b7e1fae40b2becb8e271b63316587f6a8))

- **core**: Scope runner on_event delivery to its own run
  ([`2740e7b`](https://github.com/digitl-cloud/interloper/commit/2740e7bb8588f73c2a01191986b8d28b639a4c05))

- **core**: Tear down the full dev process tree on shutdown
  ([`57b636f`](https://github.com/digitl-cloud/interloper/commit/57b636f79d8190cb54ffbffcec80b1c9773b15f0))

- **db**: Refresh organisation before expunge in accept_invitation
  ([`3285515`](https://github.com/digitl-cloud/interloper/commit/3285515c40f9d198152d28b95c78dddd485a0c2a))

- **db**: Stamp the job's last_run_at when a run completes
  ([`2e70c8e`](https://github.com/digitl-cloud/interloper/commit/2e70c8e6becfffcae9651340b221ae4dd0760175))

- **dev**: Make host-harness runs executable
  ([`0ad9af2`](https://github.com/digitl-cloud/interloper/commit/0ad9af26b70b2702b14ef924a1d72bb19962d075))

- **k8s**: Exclude kubernetes client 36.0.0 with broken in-cluster auth
  ([`0fd4435`](https://github.com/digitl-cloud/interloper/commit/0fd4435c4d6740cd689ce05b46f1dd065d5cdad7))

### Chores

- **db**: Drop alteration migrations, keep only fresh-provisioning bootstrap
  ([`deebf40`](https://github.com/digitl-cloud/interloper/commit/deebf4055de7bf41753da1725adbce9f982bcdd5))

### Code Style

- Collapse 3-line section comments into one-line headers
  ([`54fcfde`](https://github.com/digitl-cloud/interloper/commit/54fcfde2f0e678f73d8dc51bddce4dbff713a5eb))

- Organize interloper-core modules with comment sections
  ([`79064db`](https://github.com/digitl-cloud/interloper/commit/79064db49b54489e6710a60a67e213586760d695))

### Documentation

- Hooks feature page
  ([`891c16f`](https://github.com/digitl-cloud/interloper/commit/891c16f2cb96b4e3ffebfa2a5ddddbacc539aab0))

### Features

- Component-model consistency debts
  ([`6b3661a`](https://github.com/digitl-cloud/interloper/commit/6b3661ac89c7ba8597fa13147b2a9897149de693))

- Discriminator fields drive per-instance table names and display names
  ([`f696d27`](https://github.com/digitl-cloud/interloper/commit/f696d27a42ba4998559324cf4c9dc7eb2c7ddaae))

- Generic component store and /components API
  ([`97dcfbd`](https://github.com/digitl-cloud/interloper/commit/97dcfbd5ff1a086456453d4d704807377f70414d))

- Hook evaluator in the scheduler
  ([`18cad23`](https://github.com/digitl-cloud/interloper/commit/18cad2307a321187e904f7334a1b2b3cc5490e3e))

- Hooks page and definition-driven state columns
  ([`37d9eff`](https://github.com/digitl-cloud/interloper/commit/37d9eff3cd2f9240e317788ad894411883501c5f))

- New icon
  ([`3b29578`](https://github.com/digitl-cloud/interloper/commit/3b29578bf01d69948d5ba6a2d68a9487ab4a4386))

- One registry primitive and the Serializable/Component split
  ([`376407d`](https://github.com/digitl-cloud/interloper/commit/376407d6f5ebf64a704445eb2d54d3c0630d8a37))

- Per-instance asset table names via Source.asset_table
  ([`5e144af`](https://github.com/digitl-cloud/interloper/commit/5e144af4018de2bc75bd1181dbb7914c2c662db1))

- Realtime updates for components
  ([`dd2cfb5`](https://github.com/digitl-cloud/interloper/commit/dd2cfb5c76eebb2a0f7e1653e13cffe304f070f3))

- Registration is two entry-point groups
  ([`deface2`](https://github.com/digitl-cloud/interloper/commit/deface20d46c4199c5779bae9021f1182bcb0ba2))

- Relation fields follow the kind vocabulary
  ([`198da51`](https://github.com/digitl-cloud/interloper/commit/198da515a093940ff506063636dcf3a370c49380))

- Runs target any runnable component
  ([`a07c9f9`](https://github.com/digitl-cloud/interloper/commit/a07c9f9814f71094a4c9c53166eed5e61c31faa3))

- Super-admins join organisations without an invitation
  ([`42e0c75`](https://github.com/digitl-cloud/interloper/commit/42e0c754fe5c26d81e6790f51c0dc9507edb8808))

- Unify component persistence into components + component_relations
  ([`7a45639`](https://github.com/digitl-cloud/interloper/commit/7a45639c4f7291889b0b330b91f5c9c1dc0dbb00))

- Unify the run manifest with ComponentSpec and Job
  ([`65d2400`](https://github.com/digitl-cloud/interloper/commit/65d240024813295bad8768c87abcf76f630cd2fc))

- **app**: Consume the generic /components API
  ([`df711b3`](https://github.com/digitl-cloud/interloper/commit/df711b3c212b2968e928684eed29f158990d1c75))

- **app**: Render relation pickers and asset config from definitions
  ([`aabaa08`](https://github.com/digitl-cloud/interloper/commit/aabaa082a39edeec2c7e472824e60dd76c79fb10))

- **app**: Run any component from the UI
  ([`86afe7b`](https://github.com/digitl-cloud/interloper/commit/86afe7b98ad27fe31b23d50770476c5b6ed54804))

- **app**: Source form derives the instance name from the discriminator
  ([`1b9dbe4`](https://github.com/digitl-cloud/interloper/commit/1b9dbe43fe9e75442b8b9ee38af30e24db8ce099))

- **core**: Add Job component and catalog built-ins
  ([`4c8003f`](https://github.com/digitl-cloud/interloper/commit/4c8003f54dff6a97bef5df4218bf945fe2dfcb35))

- **core**: Assets self-describe; relation vocabularies carry declared slots
  ([`47298ad`](https://github.com/digitl-cloud/interloper/commit/47298adf9b27495f1ddc1b0a1b8ff9d3ae127ab7))

- **core**: Hook component kind
  ([`543990c`](https://github.com/digitl-cloud/interloper/commit/543990c96cc3f61e998bec43d5e809b170ca285b))

- **core**: Kind registry as the single authority on component kinds
  ([`0ff2bf6`](https://github.com/digitl-cloud/interloper/commit/0ff2bf6b95b0cc3a1fb0c2254706afc8cc6f6860))

- **core**: Self-describing component kinds
  ([`f106b72`](https://github.com/digitl-cloud/interloper/commit/f106b72f354928db755731a0b77555a5c0b7b6d2))

### Refactoring

- Dedupe entity authorization and shared API response plumbing
  ([`485d0b4`](https://github.com/digitl-cloud/interloper/commit/485d0b4f8206363a5b254394464c7467b40053d7))

- **core**: Compile runnable specs in the DAG domain
  ([`9f380d8`](https://github.com/digitl-cloud/interloper/commit/9f380d8f08a6fb0c895ebf155fae590df9b01c09))

- **core**: Oauth builtins module becomes oauth.providers
  ([`75c9a53`](https://github.com/digitl-cloud/interloper/commit/75c9a53e705bf3e4160d9f917a32d18af4e5812f))

- **core**: The target verb moves from Hook to TriggerHook
  ([`6d052b0`](https://github.com/digitl-cloud/interloper/commit/6d052b0cf4e7e36a3cd68ac44a443df1a8cdb0d7))

- **db**: Store owns its engine, one session policy, honest contracts
  ([`1473d3e`](https://github.com/digitl-cloud/interloper/commit/1473d3e3d376f03183287c6826cce11bbbdae7bb))

- **db**: Typed AssetExecution read model over the view
  ([`bb7a3a5`](https://github.com/digitl-cloud/interloper/commit/bb7a3a519553b83db27c74cb8a4223cc51309d33))

- **scheduler**: Shared controller loop, one terminal path, store-owned sessions
  ([`71c769b`](https://github.com/digitl-cloud/interloper/commit/71c769b23d4e83912bd6dc59624834af54925e59))


## v0.31.0 (2026-07-06)

### Bug Fixes

- **app**: Root redirect to /graph
  ([`472e39f`](https://github.com/digitl-cloud/interloper/commit/472e39f2001a482a3f65ced73525ebffcadf04ea))

### Features

- Bootstrap super-admins from INTERLOPER_AUTH_SUPER_ADMIN_EMAILS
  ([`123ce1d`](https://github.com/digitl-cloud/interloper/commit/123ce1d08ee446ab13af18b08e159e35f5a712e4))


## v0.30.1 (2026-07-03)

### Bug Fixes

- **chart**: Move agent values from api.agent to the root
  ([`a5297ba`](https://github.com/digitl-cloud/interloper/commit/a5297ba9b73a64283ed6555ddb0607f86d72e092))


## v0.30.0 (2026-07-03)

### Features

- Gate the agent behind settings with a configurable model
  ([`5c38108`](https://github.com/digitl-cloud/interloper/commit/5c381087ea8307171cee092fa7d4a7971c39133d))


## v0.29.0 (2026-07-03)

### Bug Fixes

- **app**: Catalog asset panel spans the full content height
  ([`4c1bed3`](https://github.com/digitl-cloud/interloper/commit/4c1bed3a0453c1a3fafb4c7a77e65838e403dec5))

### Features

- **app**: Docked agent chat panel that pushes the layout
  ([`cf81f8a`](https://github.com/digitl-cloud/interloper/commit/cf81f8a12b6934d50b65dc38573a45ade8a577f4))

- **app**: Redesign the UI to match the Claude Design system
  ([`99a4237`](https://github.com/digitl-cloud/interloper/commit/99a4237ae3ae64358af7200d8e0a1060c51c90b1))

- **app**: Remove the top mode switcher and the analytics pages
  ([`13065b4`](https://github.com/digitl-cloud/interloper/commit/13065b483365c72e495a33e0e6187f2422e41efa))


## v0.28.0 (2026-07-02)

### Bug Fixes

- **app**: Oauth popup flow reports cancel and failure to the opener
  ([`c8030bf`](https://github.com/digitl-cloud/interloper/commit/c8030bf065819957c779b762fceccb261f5352ac))

- **assets**: Google-ads connection resolves its own app creds env vars
  ([`d7a1da4`](https://github.com/digitl-cloud/interloper/commit/d7a1da444b491e63adf84ff2ec82b8b0cdcf0f95))

### Chores

- **assets**: Improve connections fields labels
  ([`6d71956`](https://github.com/digitl-cloud/interloper/commit/6d71956633a0540865a83e1654b7686c03b3cd32))

### Features

- Sync-callable run/materialize entrypoints backed by an il.run bridge
  ([`982df84`](https://github.com/digitl-cloud/interloper/commit/982df845af6e141a1fe1f9214371a9e07aee5a5b))

### Refactoring

- **assets**: Move awin advertiser id to a source FetchField
  ([`2f9adba`](https://github.com/digitl-cloud/interloper/commit/2f9adba7d93338a7b52a33e709da319790c71698))


## v0.27.0 (2026-07-01)

### Bug Fixes

- Follow redirects in OAuth token exchange
  ([`385bb7c`](https://github.com/digitl-cloud/interloper/commit/385bb7cbfd8323d3f742e77316ed3a7301883356))

### Features

- **assets**: Add Google Ads OAuth sign-in; scope developer tokens per connection
  ([`304db0d`](https://github.com/digitl-cloud/interloper/commit/304db0d1abba98622db6c5a6864540054a9ecccb))


## v0.26.0 (2026-07-01)

### Documentation

- Add a comment-sparingly convention to AGENTS.md
  ([`0313bba`](https://github.com/digitl-cloud/interloper/commit/0313bbac51c0330f6d99f906bb6ad64e7cb9687d))

### Features

- **assets**: Make service_account_key a JsonField instead of a SecretField
  ([`d5a74d0`](https://github.com/digitl-cloud/interloper/commit/d5a74d089c40a7e92a1eaf2026caa4ea9f90859c))

### Refactoring

- Make OAuth credential fields required, env-injected before validation
  ([`31bdb94`](https://github.com/digitl-cloud/interloper/commit/31bdb9427ec06d75014f3eeb09da79b462927936))


## v0.25.1 (2026-07-01)

### Bug Fixes

- **chart**: Make INTERLOPER_POSTGRES_PASSWORD secret ref optional
  ([`4940685`](https://github.com/digitl-cloud/interloper/commit/494068569cf580cb25b5db7778b31363200784e4))


## v0.25.0 (2026-07-01)

### Chores

- **ci**: Add optional force bump input to release workflow
  ([`2396336`](https://github.com/digitl-cloud/interloper/commit/23963362471a759b3a2f69006a2f317f427acde7))

### Features

- In-house developer_token fallback for Bing Ads sign-in
  ([`9837379`](https://github.com/digitl-cloud/interloper/commit/98373792420047701ad0cef1bbaf8dceab87a46e))

- **chart**: Add extraEnvFrom hook for api and scheduler
  ([`8cb0ac9`](https://github.com/digitl-cloud/interloper/commit/8cb0ac9f21c2ae86d9099c3f308bef73da81f388))


## v0.24.1 (2026-06-30)

### Refactoring

- Prefix connector OAuth provider env vars with INTERLOPER_
  ([`c537d35`](https://github.com/digitl-cloud/interloper/commit/c537d352102089eb7a95e1240aa219c5293ce781))

- Split OAuth connection bases and drive the form from a fields mapping
  ([`3b37f2d`](https://github.com/digitl-cloud/interloper/commit/3b37f2d0c866c0ea3be5eff4fcc87c70e8a2ea7b))


## v0.24.0 (2026-06-30)

### Features

- Documentation site at docs.interloper.dev
  ([`ad2d0d1`](https://github.com/digitl-cloud/interloper/commit/ad2d0d1f08c8441726133faa9afa949a71431a6a))


## v0.23.1 (2026-06-29)

### Bug Fixes

- Hide OAuth app credentials in sign-in mode
  ([`7fa55ba`](https://github.com/digitl-cloud/interloper/commit/7fa55ba0442ab0d70ecf5df12d8edcda158343e9))


## v0.23.0 (2026-06-29)

### Bug Fixes

- Coerce partition values to dates for time-partitioned assets
  ([`ac07f4a`](https://github.com/digitl-cloud/interloper/commit/ac07f4ace15bad4affefe1ac7f9653e88921c5e1))

- Expose annotation-declared resource slots in component definitions
  ([`6200779`](https://github.com/digitl-cloud/interloper/commit/6200779a5a51e7dea39be1d5fd955fe7f1b70710))

### Features

- Async pagination for the TikTok Ads connector
  ([`5919776`](https://github.com/digitl-cloud/interloper/commit/59197764dc1b67bb9d5f6d924c5bf576a53cb1e3))

- Async-native connectors and a REST pagination architecture
  ([`d979369`](https://github.com/digitl-cloud/interloper/commit/d9793690425821c5004d728fee959448aba25254))

- Make the execution engine async-native and unify runners
  ([`bcf03f2`](https://github.com/digitl-cloud/interloper/commit/bcf03f243ff77e24f9d4cdb35c0c277c715a21c6))

- **app**: Add a run dependency graph view
  ([`3855631`](https://github.com/digitl-cloud/interloper/commit/3855631bd7fbd7da6ba663e13d1ef04e70e3b545))

### Refactoring

- Resolve FetchField options via connection provider methods
  ([`9893fa1`](https://github.com/digitl-cloud/interloper/commit/9893fa1c4886a4470af2e17c8ddfe66f983ae06f))

- **assets**: Move bing-ads account to the source and surface API errors
  ([`5cd8893`](https://github.com/digitl-cloud/interloper/commit/5cd88931675723a5b86c27e885d043ee5d4ab09d))


## v0.22.0 (2026-06-25)

### Features

- Add All / Lifecycle / Errors / Logs tabs to run events
  ([`6e8349a`](https://github.com/digitl-cloud/interloper/commit/6e8349a4ef1d2b8df73f081dc79bd40540c882f2))

- Filter run assets and events by status
  ([`3b42874`](https://github.com/digitl-cloud/interloper/commit/3b42874bfbdf83e8e822dc27b8454e30f827cbbb))

- **app**: Add run summary header card
  ([`b769104`](https://github.com/digitl-cloud/interloper/commit/b769104de1cde1eaaa2b45a6ecf58f7d84966984))


## v0.21.0 (2026-06-25)

### Bug Fixes

- JSON-encode nested values for str-typed schema fields in conformer
  ([`c548f10`](https://github.com/digitl-cloud/interloper/commit/c548f1028fb53fd19d443be42fe7a9920938ea3f))

- **app**: Focus graph only on the open-panel asset, not source expansion
  ([`7ae324b`](https://github.com/digitl-cloud/interloper/commit/7ae324b4bb33c5d6be8403145a1fff9b74f9e453))

- **app**: Tune timeline success color, unclip 0 tick, align scrollbar
  ([`9d046c6`](https://github.com/digitl-cloud/interloper/commit/9d046c66e8e816cd40192a4efcee29233e2f770f))

### Code Style

- **app**: Collapsible Materialization section with status + history
  ([`08b75b7`](https://github.com/digitl-cloud/interloper/commit/08b75b755b6226fd5349a6f66456e59fb6956069))

- **app**: Focus-on-select graph edges, blue selection, drop status dot
  ([`d69a78b`](https://github.com/digitl-cloud/interloper/commit/d69a78be93b7c2ff96372846e4e9387569cae2bc))

- **app**: Match graph node design — elevation, icon tiles, contrast
  ([`138c29b`](https://github.com/digitl-cloud/interloper/commit/138c29bb8ac7c0c7d4f3c4378e0c6ac50ddf765f))

- **app**: Match Latest materialization card to panel widgets
  ([`768f482`](https://github.com/digitl-cloud/interloper/commit/768f482ea64e167feb744c5f133c2ca67e9db9c3))

- **app**: Reorder asset-panel sections by relevance
  ([`b5bd458`](https://github.com/digitl-cloud/interloper/commit/b5bd4588af40b2d2594476a0b455134084a77146))

- **app**: Replace asset-panel metrics with a Latest materialization card
  ([`7413356`](https://github.com/digitl-cloud/interloper/commit/7413356dc1a46a64773ca32a215bf41cb18d0dc5))

- **app**: Show job name in Latest materialization meta
  ([`6f4dc5c`](https://github.com/digitl-cloud/interloper/commit/6f4dc5ced4c142e34a98b6943731057126b377a9))

- **app**: Unify graph toolbar controls, left-aligned
  ([`344642d`](https://github.com/digitl-cloud/interloper/commit/344642df9fc1a0a8589cb066640eb608676d78eb))

### Documentation

- Agents run the dev app on a non-3000 port
  ([`e2bfd3f`](https://github.com/digitl-cloud/interloper/commit/e2bfd3f8943c5a2eab18b95968bbf5878e743d5e))

### Features

- Detect and surface catalog-key drift for sources and assets
  ([`0b68718`](https://github.com/digitl-cloud/interloper/commit/0b6871873d2ed78ab74eac875e932a0f7e774d80))

- **app**: Add graph toolbar filters and Topology/Status view modes
  ([`a88f9c6`](https://github.com/digitl-cloud/interloper/commit/a88f9c604bc28625d65d87777c3ca7153294cc9b))

- **app**: Add List/Graph/Nodes expand modes, redesign source cards
  ([`85a6132`](https://github.com/digitl-cloud/interloper/commit/85a61323e38b018351e4c094a44943c18618d807))

- **app**: Enrich asset detail panel with status, metrics, runs
  ([`178ff64`](https://github.com/digitl-cloud/interloper/commit/178ff648c23fc36a628798ea77b4563ca4f89400))

- **app**: Rewrite ExecutionTimeline as a native Gantt chart
  ([`6c47f24`](https://github.com/digitl-cloud/interloper/commit/6c47f242d7d7204c55ff4240a8e68e93caf54dca))

### Refactoring

- Drop "metadata" asset terminology in favor of "entity"
  ([`5c85624`](https://github.com/digitl-cloud/interloper/commit/5c85624929ce79e5b02a62a45d00fe122c57fa4d))

- **app**: Extract reusable GraphCanvas from AssetGraph
  ([`b08b76e`](https://github.com/digitl-cloud/interloper/commit/b08b76e1169967c66a3bea72f7418f9987ae1a80))


## v0.20.0 (2026-06-23)

### Chores

- Remove oauthconnection field description
  ([`3b0c781`](https://github.com/digitl-cloud/interloper/commit/3b0c7811e59af23d756a2ed2a563f999e5f577b9))

### Documentation

- Document the asset naming/tag convention in AGENTS.md
  ([`86657b2`](https://github.com/digitl-cloud/interloper/commit/86657b2691ace4a6a48dc1c6ec1610d75ba9d866))

### Features

- **app**: Add interactive column sorting to client-side tables
  ([`10e774e`](https://github.com/digitl-cloud/interloper/commit/10e774e3863d8fee268d32677908a426705ad83b))

### Refactoring

- Apply _stats convention to amazon_ads reports
  ([`68cec68`](https://github.com/digitl-cloud/interloper/commit/68cec68d4c49001c8e3fe8b20722014b1417e5f8))


## v0.19.0 (2026-06-22)

### Bug Fixes

- **ci**: Build frontend SPA on native arch to stop multi-arch hang
  ([`ba58c24`](https://github.com/digitl-cloud/interloper/commit/ba58c2488223d0eb8aa869988e69341bfc5180dd))

### Features

- **app**: Add type column with logo to sources and destinations tables
  ([`782a2a8`](https://github.com/digitl-cloud/interloper/commit/782a2a8a1e13e2a7b8261c65e85b3a2b2a8f126a))


## v0.18.0 (2026-06-22)

### Bug Fixes

- Bump tailwindcss to 4.3.1 to silence Node 26 module.register deprecation
  ([`16cbc1e`](https://github.com/digitl-cloud/interloper/commit/16cbc1e962cfdbd29760b03d9cdfee55ccaab4a3))

- Encrypt resources by default and fail closed without an encryption key
  ([`9f59aa5`](https://github.com/digitl-cloud/interloper/commit/9f59aa5495b8d99263f288563789eb131f468429))

- Resolve in-house oauth app secret from env, never ship it to the browser
  ([`b2dc156`](https://github.com/digitl-cloud/interloper/commit/b2dc156968c9b268f157b2bab31c3860606a952a))

### Documentation

- Add asset naming map (interloper <-> digitlcloud-connectors)
  ([`1e6017c`](https://github.com/digitl-cloud/interloper/commit/1e6017c1fe5e72bf4310b0342df4faa45e28a6c2))

### Features

- Gate org-bespoke content on the selected organisation
  ([`6f418c3`](https://github.com/digitl-cloud/interloper/commit/6f418c326be0dacff814fa38ff6d5ef5c306871a))

### Refactoring

- Publish one image per role, flavors as tag suffixes
  ([`7f8a465`](https://github.com/digitl-cloud/interloper/commit/7f8a465d2eda113fc8416070847100fa1edad5fb))

- Reduce unimplemented (fake_data) sources to empty source classes
  ([`54327f0`](https://github.com/digitl-cloud/interloper/commit/54327f09e46a67fa5ccdc00e57bc97700b80e958))

- Rename assets to stats/entity/event convention
  ([`aba8ac8`](https://github.com/digitl-cloud/interloper/commit/aba8ac8d1717ef7552049824faaf107d71b89a5a))


## v0.17.0 (2026-06-18)

### Bug Fixes

- Coerce tiktok stat_time_day to a date for the destination
  ([`4b03e16`](https://github.com/digitl-cloud/interloper/commit/4b03e16fbed4dce7f2df66368c76f1a14f558cf9))

- **api**: Install agent extra in api image so /agent routes mount
  ([`0c26689`](https://github.com/digitl-cloud/interloper/commit/0c266892d8d49f007726c4774a59389f1113d236))

### Features

- Enable oauth sign-in for tiktok ads connection
  ([`08124ee`](https://github.com/digitl-cloud/interloper/commit/08124ee3bf6d14b49dd4f660ebd3641f000207ec))

- **api**: Add criteo/advertisers FetchField endpoint
  ([`8032ba3`](https://github.com/digitl-cloud/interloper/commit/8032ba3845d62f53062ffa1d3662065262fb0ab8))


## v0.16.0 (2026-06-17)

### Bug Fixes

- Exclude framework resources field from asset schema
  ([`5e7ff08`](https://github.com/digitl-cloud/interloper/commit/5e7ff08372953a7e8c960c5dedd3ecec117ddb64))

- Normalize columns + return native records for adservice/adup/awin
  ([`5e96949`](https://github.com/digitl-cloud/interloper/commit/5e9694958c00bbddcce56b5521c14f2ff5a8718c))

- Normalize mixed-timezone datetimes to UTC in the conformer
  ([`a583e80`](https://github.com/digitl-cloud/interloper/commit/a583e805566d175d14c4b8f1a8858a42ae2255c1))

- Port the full Bing Ads report schema
  ([`d366b59`](https://github.com/digitl-cloud/interloper/commit/d366b59741a757b941f344a61b05033414a7ed4a))

- Resolve .env from working directory in CLI
  ([`fc2f51e`](https://github.com/digitl-cloud/interloper/commit/fc2f51ef71f451c1de250705b8e9868713394de6))

### Chores

- **google-cloud**: Read_representation on decorator
  ([`fdb2ace`](https://github.com/digitl-cloud/interloper/commit/fdb2ace4a5691ab3d208e6be3874c3b8abfd98bf))

### Features

- Add impact/programs external API route
  ([`ac4ef74`](https://github.com/digitl-cloud/interloper/commit/ac4ef7418a658b3307ffe5ef3bc5d5392784b3bd))

- Implement bing_ads ad performance report
  ([`c94df6e`](https://github.com/digitl-cloud/interloper/commit/c94df6e1182801cf356729da495d753e985e685b))

- Implement criteo source
  ([`a218acd`](https://github.com/digitl-cloud/interloper/commit/a218acd889bb9b9fe8ae49f11451a8743b0769cf))

- Implement facebook ads source
  ([`1fa674b`](https://github.com/digitl-cloud/interloper/commit/1fa674bb66c3763ae42686910f692ee7e29e658c))

- Implement impact source
  ([`a9ae182`](https://github.com/digitl-cloud/interloper/commit/a9ae182baabe169947b2e17c837d3dfe955ca041))

- Implement replace_empty_strings/replace_empty_dicts normalizer options
  ([`0e5d268`](https://github.com/digitl-cloud/interloper/commit/0e5d268865e2408666d587e8d4fc0bdcef726fff))

- Implement snapchat ads source
  ([`716bdf3`](https://github.com/digitl-cloud/interloper/commit/716bdf3a18f01f283b73c34d766d10d99c17c7bc))

- Implement tiktok ads source
  ([`ffe4c45`](https://github.com/digitl-cloud/interloper/commit/ffe4c452e8977544eda9ad993d780965a7ac0fc4))

- Manifest resources/destinations registry with refs and auto-use
  ([`08a468f`](https://github.com/digitl-cloud/interloper/commit/08a468f559d780f35fcdf66681aed32b59a1cf5a))

- **api**: Add tiktok-ads/advertisers FetchField endpoint
  ([`b1ef2c7`](https://github.com/digitl-cloud/interloper/commit/b1ef2c7990aecf4e585ebf9482f9927134ccb8dd))

### Refactoring

- Inline impact report-export IDs into the assets
  ([`e6d52e3`](https://github.com/digitl-cloud/interloper/commit/e6d52e358aa7a632fdec6511ba20f5e02d5dff80))

- Move facebook actions-pivot into a custom normalizer
  ([`845c459`](https://github.com/digitl-cloud/interloper/commit/845c459e301c01f50b5bfd5e2ba567bdd30c25a7))

- Move snapchat report framing into a custom normalizer
  ([`a5fef78`](https://github.com/digitl-cloud/interloper/commit/a5fef786959d75ad5790bbbda269d6c04d420457))

- Return native records from amazon/criteo assets
  ([`2ad0e36`](https://github.com/digitl-cloud/interloper/commit/2ad0e3617f79db6cb062578b033c86c51a603d30))


## v0.15.1 (2026-06-12)

### Bug Fixes

- Route resource kwargs into slots and type-check by-name trickle
  ([`e7540ce`](https://github.com/digitl-cloud/interloper/commit/e7540cef7d275e570d1ac4ad10fe2b40f0857ee9))

### Chores

- Sync uv.lock with released workspace versions
  ([`7e818c0`](https://github.com/digitl-cloud/interloper/commit/7e818c0e75c3ed34cfd81d2d04373edef7e84dc7))

### Continuous Integration

- Regenerate uv.lock in the release commit
  ([`cbf2b27`](https://github.com/digitl-cloud/interloper/commit/cbf2b276c398d3183f2d98104bbede2b1b3eceb7))


## v0.15.0 (2026-06-12)

### Features

- Declarative run manifest for ad-hoc DAG materialization
  ([`aa53003`](https://github.com/digitl-cloud/interloper/commit/aa53003c041fa63fdcfd16ed020650a0eedc3d9c))

- Stream run events through the logger in the run CLI
  ([`43ddd59`](https://github.com/digitl-cloud/interloper/commit/43ddd594d56d844892fdb9618b8a9409246ffa0b))

### Testing

- Drain in-flight events before capturing in k8s terminal-event tests
  ([`082d22a`](https://github.com/digitl-cloud/interloper/commit/082d22a021466071d97edbdb89e1d3c35164fbe4))


## v0.14.0 (2026-06-11)

### Bug Fixes

- List BigQuery projects via the BigQuery API instead of Resource Manager
  ([`bfbac09`](https://github.com/digitl-cloud/interloper/commit/bfbac099e1771370a35cceac6d2c0f2deda3cea3))

### Continuous Integration

- Trigger publish on release published, not the tag push
  ([`fcbacfb`](https://github.com/digitl-cloud/interloper/commit/fcbacfbbb234d949b43288ecd9ca11724441dc1c))

### Features

- Surface Google's error message in the projects fetch route
  ([`ed367bd`](https://github.com/digitl-cloud/interloper/commit/ed367bd1863c5449cdaed33e21c3dbf7416d4c5b))


## v0.13.0 (2026-06-11)

### Continuous Integration

- Split docker/helm publishing into a tag-triggered publish workflow
  ([`b7ecc6d`](https://github.com/digitl-cloud/interloper/commit/b7ecc6d4536013b159763987e26d03432937870f))

### Features

- Fetch BigQuery project options dynamically from the connection
  ([`24137b7`](https://github.com/digitl-cloud/interloper/commit/24137b7b15ec4f5d568add0e921c374228c48869))

- Oauth provider registry, OAuthConnection base, and decorator-level oauth config
  ([`5f6745b`](https://github.com/digitl-cloud/interloper/commit/5f6745bbe0134e969f8173ff31764717c6a88c07))


## v0.12.0 (2026-06-10)

### Bug Fixes

- Load filtered events from the server when a timeline asset is selected
  ([`dcb9f6c`](https://github.com/digitl-cloud/interloper/commit/dcb9f6c7ac0b921cad5a73d97ba02cda8560c875))

### Features

- Persist and display log event level
  ([`74456f0`](https://github.com/digitl-cloud/interloper/commit/74456f0a558f8eba332b9fdb2da8ce2c8d6e1747))

### Refactoring

- Destination backend traits are class-level, not instance config
  ([`7da3999`](https://github.com/digitl-cloud/interloper/commit/7da3999bc60f04cf658f6a7044fc5cd56885b883))

- Partition dispatch as a destination base-class template
  ([`64f5243`](https://github.com/digitl-cloud/interloper/commit/64f524347f469bd526318f133d372c4b7a1f6cd5))


## v0.11.0 (2026-06-10)

### Features

- Write schema descriptions and partitioning to BigQuery tables
  ([`8e6f665`](https://github.com/digitl-cloud/interloper/commit/8e6f665477516903b63633c1810d8420a77178c7))

### Refactoring

- Catalog discovery via entry points
  ([`80f65dd`](https://github.com/digitl-cloud/interloper/commit/80f65ddae1b88d5011574d3f6c4a9b8c4bf049b4))

- Launcher registry via entry points
  ([`2c9d1a4`](https://github.com/digitl-cloud/interloper/commit/2c9d1a43927798420d7b7cb56568ac1a6785374d))

- One conform seam — Conformer resolved from the data's representation
  ([`81176b1`](https://github.com/digitl-cloud/interloper/commit/81176b1bc5fa4fd8606352c4bca22ca5fe452b97))

- Representation seam — pandas-agnostic core, DataAdapter retired
  ([`52eed48`](https://github.com/digitl-cloud/interloper/commit/52eed481c168de6e9c237ca13eda0450669ef93c))

- Runner registry via entry points; canonicalize the kubernetes key
  ([`5ff0ba6`](https://github.com/digitl-cloud/interloper/commit/5ff0ba6ee1f8f00e063ca9868c8768036f8aed4b))


## v0.10.1 (2026-06-10)

### Bug Fixes

- Carry the asset's terminal error into run-level failures (k8s)
  ([`6efe731`](https://github.com/digitl-cloud/interloper/commit/6efe73188b64d73ca3b8a3af7461b316b52d5610))

- Decorators build through the metaclass; Normalizer becomes a Component
  ([`df457ab`](https://github.com/digitl-cloud/interloper/commit/df457ab2af3d4801a5a61b548a6dbfe840236253))

- Timeline asset clicks not filtering events while a run is live
  ([`04682e0`](https://github.com/digitl-cloud/interloper/commit/04682e00bf4959303d8023ae7815b23112ce6675))


## v0.10.0 (2026-06-10)

### Chores

- Migrate type checking from pyright to ty
  ([`9c921eb`](https://github.com/digitl-cloud/interloper/commit/9c921eb8bfadf8adf8389495bcd9913fbefc7d03))

- Update deps
  ([`2757963`](https://github.com/digitl-cloud/interloper/commit/275796352e81e16ab16c0f38d1d1402d5a33e44e))

### Features

- Schema-driven destinations with always-on conform and native DataFrame loads
  ([`f209e24`](https://github.com/digitl-cloud/interloper/commit/f209e2447cd660783249bbbd006862a8589c7148))


## v0.9.0 (2026-06-09)

### Bug Fixes

- Attribute LOG events to their asset so they filter and label correctly
  ([`f9d5d65`](https://github.com/digitl-cloud/interloper/commit/f9d5d651183b99bb47c36de0f7f2f8104de80f06))

### Features

- Skip destination writes when an asset produces no data
  ([`57d26b3`](https://github.com/digitl-cloud/interloper/commit/57d26b349cf9e0908f49172acc51df3e3bfcd6d0))


## v0.8.0 (2026-06-08)

### Bug Fixes

- Create missing runs/events indexes and bound migration lock waits
  ([`1fb1e86`](https://github.com/digitl-cloud/interloper/commit/1fb1e86df31794dd2b10e79cd25de4a1c7d8d570))

- Stop infinite-scroll flicker at the end of the events list
  ([`300d2d2`](https://github.com/digitl-cloud/interloper/commit/300d2d2353a851906c96d216f7822fad9c01faa5))

- **db**: Apply CONCURRENTLY-index migrations during db init/reset
  ([`8d4fe41`](https://github.com/digitl-cloud/interloper/commit/8d4fe411fe7efb6402d9fed7a365e3312969f0a0))

### Features

- Local dev harness with seeded instance (host + docker-compose)
  ([`787c961`](https://github.com/digitl-cloud/interloper/commit/787c961d9da3d674d1b8a2969ea2c7f800ce8716))


## v0.7.0 (2026-06-05)

### Bug Fixes

- Author terminal asset events from the host to stop orphaned executions
  ([`5ea5667`](https://github.com/digitl-cloud/interloper/commit/5ea5667e75dc915b511b2950b84f5dc442f2a24f))

- Paginate run events so outcome events are reachable
  ([`e7ae96f`](https://github.com/digitl-cloud/interloper/commit/e7ae96fe6a57c691146128f50fce80bd4c022dee))

### Documentation

- Document worktree naming convention in AGENTS.md
  ([`06dc98e`](https://github.com/digitl-cloud/interloper/commit/06dc98ebd6be249424b03b7591296c46cdcfa097))

- Require squashing branch commits as progress is made
  ([`3819396`](https://github.com/digitl-cloud/interloper/commit/3819396ab23b807edb1c32b6622fa9a477f6f970))

### Features

- Add retry for failed runs
  ([`d0b0934`](https://github.com/digitl-cloud/interloper/commit/d0b093440b74a592c34b730a9058a36f08b8d897))

- Assign stable event ids and persist events idempotently
  ([`ae0bd90`](https://github.com/digitl-cloud/interloper/commit/ae0bd90f72753ffbcf01a224e00f2a7e1c6fd411))


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
