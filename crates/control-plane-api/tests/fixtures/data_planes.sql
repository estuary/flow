do $$
declare
  data_plane_one_id flowid := '111111111111';
  data_plane_two_id flowid := '222222222222';

  -- SOPS-encrypted HMAC keys (see .cargo/config.toml for key & secret)
  -- Decrypts to: {"hmac_keys": ["c2VjcmV0", "b3RoZXI="]} (base64 for "secret" and "other")
  encrypted_keys json := $sops${
	"hmac_keys": [
		"ENC[AES256_GCM,data:kgzvBY4pczo=,iv:lA0c8tGWTC866p25il82vspY50Kl8oGWqJzNaw/zWyc=,tag:xA68U8wdFjlixqI8/zjQkA==,type:str]",
		"ENC[AES256_GCM,data:q5Y5mbUzwRQ=,iv:bla/xToMmLdlf9N7z9Z2pxyWmpa0lQf9wizKspnyaTU=,tag:CR2QyhoO5crrDM6ZRoPPIw==,type:str]"
	],
	"sops": {
		"age": [
			{
				"recipient": "age1z2qskpk8ww2rx5frxzykryufkfm9sj03v56m3qu54y0lgndytp3sdg24vs",
				"enc": "-----BEGIN AGE ENCRYPTED FILE-----\nYWdlLWVuY3J5cHRpb24ub3JnL3YxCi0+IFgyNTUxOSBtZ2ZwQks2T2Zna3h4OFov\nVjJTSzl4TGVyRnIrVk40bndWNVVmZFJQcXdZCkZpY3BlYnRKWUhZYlFnNk50dHh3\nbHlNY0R2bFI1S3pKVkJhbURHM3hYZFEKLS0tIDR0OTMrZEJkRHhKb0FaaXV1K2Uy\nNFd0bHd4TGFMK0xFUUFQVUtta3JHTlEKFBocCSHvSypsA7UcD7emwNDDS4Q3g2si\nqG+pyMMp2yObmT5iN/VAodv4un8Vr1LvcsKxEGXwTOjOnFCAk+I1Kw==\n-----END AGE ENCRYPTED FILE-----\n"
			}
		],
		"lastmodified": "2026-01-15T01:29:57Z",
		"mac": "ENC[AES256_GCM,data:LTZlM4OPKZRu4J9HYUHn5fF6OBP8APXibP47sa/ADpMPA09h1q7Ivg+EAwJlqX9Ee9h8B7iEZD6CMxEyuz94T+nqiklBt4jZIQYq9GPd4xBp1EUvzSITmkvILYkkHh+A+vYU0cH/CemPv/w87wNt+QeNtsQj2xowufsr7RXCyXQ=,iv:z7t+spQvkzLMZZ3LA3eWmtZZdWKPVghKCP5M3HsQ6og=,tag:zrgfUxvTWsRuC8/2kEu6zw==,type:str]",
		"unencrypted_suffix": "_unencrypted",
		"version": "3.11.0"
	}
}$sops$;

begin

  insert into public.data_planes (
    id,
    data_plane_name,
    data_plane_fqdn,
    hmac_keys,
    encrypted_hmac_keys,
    broker_address,
    reactor_address,
    ops_logs_name,
    ops_stats_name,
    ops_l1_events_name,
    ops_l1_inferred_name,
    ops_l1_stats_name,
    ops_l2_events_transform,
    ops_l2_inferred_transform,
    ops_l2_stats_transform,
    enable_l2
  ) values (
    data_plane_one_id,
    'ops/dp/public/one',
    'dp.one',
    '{}',
    encrypted_keys,
    'broker.dp.one',
    'reactor.dp.one',
    'ops/tasks/public/one/logs',
    'ops/tasks/public/one/stats',
    'ops/rollups/L1/public/one/events',
    'ops/rollups/L1/public/one/inferred',
    'ops/rollups/L1/public/one/stats',
    'from.dp.one',
    'from.dp.one',
    'from.dp.one',
    true
  ), (
    data_plane_two_id,
    'ops/dp/public/two',
    'dp.two',
    '{}',
    encrypted_keys,
    'broker.dp.two',
    'reactor.dp.two',
    'ops/tasks/public/two/logs',
    'ops/tasks/public/two/stats',
    'ops/rollups/L1/public/two/events',
    'ops/rollups/L1/public/two/inferred',
    'ops/rollups/L1/public/two/stats',
    'from.dp.two',
    'from.dp.two',
    'from.dp.two',
    true
  );

end
$$;
