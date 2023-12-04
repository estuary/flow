
begin;

create table oauth_flows (
    "state" 				text primary key,
    "connector_id" 			flowid,
    "code_verifier" 		text not null,
    "code_challenge" 		text not null,
    "code_challenge_method" text not null,
	"extra"					jsonb
);

commit;

