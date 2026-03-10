import type { CodegenConfig } from '@graphql-codegen/cli'

const config: CodegenConfig = {
  schema: 'schema/control-plane-api.graphql',
  generates: {
    'src/types.ts': {
      plugins: ['typescript'],
      config: {
        scalars: {
          DateTime: 'string',
          Id: 'string',
          JSON: 'unknown',
          JSONObject: 'Record<string, unknown>',
          UUID: 'string',
          Prefix: 'string',
          Name: 'string',
          Collection: 'string',
          Url: 'string',
        },
        enumsAsTypes: true,
        skipTypename: true,
      },
    },
  },
}

export default config
