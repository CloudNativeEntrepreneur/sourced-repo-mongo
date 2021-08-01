module.exports = {
  env: {
    commonjs: true,
    es2021: true,
    node: true
  },
  extends: [
    'standard'
  ],
  globals: {
    gql: true,
    ga: true,
    describe: true,
    it: true,
    test: true,
    expect: true,
    jest: true,
    before: true,
    beforeEach: true,
    after: true,
    afterEach: true,
    jasmine: true,
    beforeAll: true,
    afterAll: true
  },
  parserOptions: {
    ecmaVersion: 12
  },
  rules: {
    'node/no-deprecated-api': 'off'
  }
}
