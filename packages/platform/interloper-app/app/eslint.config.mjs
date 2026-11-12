// @ts-check
import withNuxt from "./.nuxt/eslint.config.mjs"

export default withNuxt({
  rules: {
    "@typescript-eslint/no-unused-vars": "off",
    "@typescript-eslint/no-explicit-any": "off",
    "@typescript-eslint/ban-ts-comment": "off",
    "vue/multi-word-component-names": "off",
    "vue/first-attribute-linebreak": ["warn", {
      singleline: "ignore",
      multiline: "beside"
    }],
    "vue/valid-v-slot": ["error", {
      allowModifiers: true,
    }],
    // The design rules (.claude/rules/app-design.md) that a class name alone can break.
    "vue/no-restricted-class": ["error",
      // Arbitrary values: sizes, spacing, radii and colours come from the theme's scales.
      "/(^|:)[a-z0-9-]+-\\[[^\\]]*\\]$/",
      // Palette colours and white/black: colours are semantic tokens.
      "/(^|:)(text|bg|border|ring|outline|divide|fill|stroke|from|via|to|shadow|decoration|accent|caret)-(slate|gray|zinc|neutral|stone|red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose)-\\d{2,3}(\\/\\d+)?$/",
      "/(^|:)(text|bg|border|ring)-(white|black)(\\/\\d+)?$/",
      // Dark mode comes from the tokens.
      "/(^|:)dark:/",
      // One family, three weights; capitals and tracking only through the `caption` class.
      "/(^|:)font-(serif|bold|extrabold|black)$/",
      "/(^|:)tracking-/",
      "/(^|:)uppercase$/",
    ],
  },
})
