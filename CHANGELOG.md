# CHANGELOG

<!-- version list -->

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
