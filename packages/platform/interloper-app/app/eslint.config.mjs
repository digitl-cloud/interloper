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
  },
})
