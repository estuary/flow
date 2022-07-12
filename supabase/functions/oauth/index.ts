import {serve} from "https://deno.land/std@0.131.0/http/server.ts"

import {accessToken} from './access-token.ts';
import {authURL} from './auth-url.ts';
import {encryptConfig} from './encrypt-config.ts';

console.log("Hello from Functions!")

serve(async (req) => {
  const request = await req.json()

  if (request.operation == 'auth-url') {
    return authURL(request);
  }
  else if (request.operation == 'access-token') {
    return accessToken(request);
  }
  else if (request.operation == 'encrypt-config') {
    return encryptConfig(request);
  }
  else {
    return new Response(
        JSON.stringify({"error" : "unknown_operation"}),
        {headers : {"Content-Type" : "application/json"}, status : 400},
    )
  }
})
