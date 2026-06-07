-- callback_task 增加重试相关字段
alter table if exists callback_task
    add column if not exists retry_count int not null default 0;

alter table if exists callback_task
    add column if not exists max_retry_count int not null default 3;

alter table if exists callback_task
    add column if not exists next_retry_time timestamp null;

create index if not exists idx_callback_task_next_retry_time
    on callback_task (status, next_retry_time);

-- 新增 outbox 表
create table if not exists callback_outbox (
    id uuid primary key,
    task_id uuid not null,
    topic_name varchar(128) not null,
    message_key varchar(128) not null,
    message_payload jsonb not null,
    status varchar(32) not null,
    publish_retry_count int not null default 0,
    next_attempt_time timestamp not null,
    last_error_msg varchar(255) null,
    created_at timestamp not null default current_timestamp,
    updated_at timestamp not null default current_timestamp,
    constraint fk_callback_outbox_task
        foreign key (task_id) references callback_task (id)
);

create index if not exists idx_callback_outbox_status_next_attempt
    on callback_outbox (status, next_attempt_time);

create index if not exists idx_callback_outbox_task_id
    on callback_outbox (task_id);
