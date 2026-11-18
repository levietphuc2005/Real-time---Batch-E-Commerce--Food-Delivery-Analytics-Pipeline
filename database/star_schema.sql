create table if not exists dim_customer(
	customer_key serial primary key,
	customer_id varchar(50) unique not null,
	created_at timestamp default current_timestamp
)

create table if not exists dim_item(
	item_key serial primary key,
	item_name varchar(100) unique not null,
	standard_price numeric(10,2)
)

create table if not exists dim_date(
	date_key serial primary key,
	full_date date unique not null,
	year int not null,
	quarter int not null,
	month int not null,
	day int not null,
	day_of_week varchar(20) not null
)

create table if not exists fact_orders(
	order_id varchar(50) primary key,
	customer_key int references dim_customer(customer_key),
	item_key int references dim_item(item_key),
	date_key int references dim_date(date_key),
	price numeric(10,2),
	quantity int,
	total_amount numeric(10,2),
	status varchar(50),
	order_timestamp timestamp
)

create index if not exists idx_fact_customer_key on fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_item_key ON fact_orders(item_key);
CREATE INDEX IF NOT EXISTS idx_fact_date_key ON fact_orders(date_key);

