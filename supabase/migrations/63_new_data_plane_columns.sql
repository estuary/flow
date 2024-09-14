begin;

alter table data_planes add column aws_iam_user_arn text;
alter table data_planes add column cidr_blocks cidr[] not null default '{}';
alter table data_planes add column enable_l2 boolean not null default false;
alter table data_planes add column gcp_service_account_email text;
alter table data_planes add column ssh_private_key text;

-- Must be provided explicitly.
alter table data_planes alter column enable_l2 drop default;

-- Users may read out details of applied data-plane configuration.
grant select (
    aws_iam_user_arn,
    cidr_blocks,
    gcp_service_account_email
) on data_planes to authenticated;

commit;