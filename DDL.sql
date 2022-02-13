DROP FUNCTION IF EXISTS chirkova.create_table();
CREATE OR REPLACE FUNCTION chirkova.create_table()
returns text
AS $$
declare 
st_dt DATE;
ed_dt DATE;
min_val_ac date;
max_val_ac date;
min_val_pas date;
max_val_pas date;
begin
/* Создание таблицы фактов со следующими ограничениями:
 * Данные не должны принимать значения null
 * Тип оперции принимает значения: 'Пополнение','Снятие','Оплата'
 * Результат операции принимает значения: 'Успешно','Отказ'
 * Сумма транзакции не может принимать отрицательные значения
 * В качестве ключа распределения выбрана колонка trans_id, которая принимает уникальные значения
 */
--Дата начала месяца, дата начала следующего месяца, необходимо для последущего партиционирования по дням
select date( to_timestamp(to_char(trans_date,'YYYY-DD-MM HH24:MI:SS'),'YYYY-MM-DD HH24:MI:SS')),
	   date( to_timestamp(to_char(trans_date,'YYYY-DD-MM HH24:MI:SS'),'YYYY-MM-DD HH24:MI:SS')) + interval '1 month'
into  st_dt,ed_dt
from (select trans_date from STG_data order by trans_date limit 1) a;

--Минимальные и максимальные значения даты окончания договора, и даты окончания действия паспорта соответственно
select min(valid_to), max(valid_to) from STG_data into min_val_ac,max_val_ac;
select min(passport_valid_to), max(passport_valid_to) from STG_data into min_val_pas,max_val_pas;

--Создание сиквенсов для генерации сурогатных ключей
DROP SEQUENCE IF EXISTS chirkova.terminals_seq;
CREATE SEQUENCE chirkova.terminals_seq 
START WITH 1
CACHE 100
NO CYCLE;

DROP SEQUENCE IF EXISTS chirkova.dim_cards_directory_seq;
CREATE SEQUENCE chirkova.dim_cards_directory_seq
START WITH 1
CACHE 1000
NO CYCLE;

DROP SEQUENCE IF EXISTS chirkova.dim_accounts_seq;
CREATE SEQUENCE chirkova.dim_accounts_seq
START WITH 1
CACHE 1000
NO CYCLE;

DROP SEQUENCE IF EXISTS chirkova.dim_clients_seq;
CREATE SEQUENCE chirkova.dim_clients_seq
START WITH 1
CACHE 500
NO CYCLE;

DROP SEQUENCE IF EXISTS chirkova.dim_cards_seq;
CREATE SEQUENCE chirkova.dim_cards_seq
START WITH 1
CACHE 1000
NO CYCLE;


/* Создание таблицы фактов со следующими ограничениями:
 * Данные не должны принимать значения null
 * Тип оперции принимает значения: 'Пополнение','Снятие','Оплата'
 * Результат операции принимает значения: 'Успешно','Отказ'
 * Сумма транзакции не может принимать отрицательные значения
 * В качестве ключа распределения выбрана колонка trans_id, которая принимает уникальные значения
 * Партиционнирование по днем месяца
 */
EXECUTE	
'DROP TABLE IF EXISTS chirkova.FACT_TRANSACTIONS;
create table chirkova.FACT_TRANSACTIONS(
	trans_id 			varchar(9) not null,
	trans_date 			timestamp not null,
	card_key 			integer not null,
	oper_type 			varchar(20) check (oper_type in (''Пополнение'',''Снятие'',''Оплата'')),
	amt 				decimal check(amt>0),
	oper_result 		varchar(20) check (oper_result in (''Успешно'',''Отказ'')),
	terminal_key 		integer not null
)
with(
	APPENDONLY=TRUE,
    COMPRESSTYPE=QUICKLZ,
    ORIENTATION=ROW
)

DISTRIBUTED BY (trans_id)
PARTITION BY range (trans_date)
(START (date '''|| st_dt||''') INCLUSIVE
    END (date '''|| ed_dt||''') EXCLUSIVE
    EVERY (INTERVAL ''1 day''));';


/* Создание таблицы измерений 'terminal', данные всех колонок данной таблицы не должны принимать значения null
 * В качестве ключа распределения выбрана колонка terminal_key, которая принимает уникальные значения
 */

DROP TABLE IF EXISTS chirkova.dim_terminals;
create table chirkova.dim_terminals(
	terminal_key        integer not null,
	terminal_id 		varchar(8) not null,
	terminal_type 		varchar(3) not null,
	terminal_city 		varchar(30) not null,
	terminal_address 	varchar(60) not null,
	start_dt 		    timestamp  DEFAULT now(),
	end_dt              timestamp  DEFAULT null
)
DISTRIBUTED BY (terminal_key);

/* Создание таблицы справочника по номерам карт, данные всех колонок данной таблицы не должны принимать значения null, версионность не поддерживается
 */

DROP TABLE IF EXISTS chirkova.dim_cards_directory;
create table chirkova.dim_cards_directory(
	card_dir_key			integer not null,
    card_num                varchar(20) not null
)
DISTRIBUTED BY (card_dir_key);


/* Создание таблицы измерений 'cards', данные всех колонок данной таблицы не должны принимать значения null
 * В качестве ключа распределения выбрана колонка card_key, которая принимает уникальные значения
 */

DROP TABLE IF EXISTS chirkova.DIM_cards;
create table chirkova.DIM_cards(
	card_key            integer not null,
	card_dir_key		integer not null,
	account_key 		integer not null,
	start_dt 		    timestamp  DEFAULT now(),
	end_dt              timestamp  DEFAULT null
)
DISTRIBUTED BY (card_key);

/* Создание таблицы измерений 'accounts', данные всех колонок данной таблицы не должны принимать значения null
 * В качестве ключа распределения выбран суррогатный ключ account_key, которая принимает уникальные значения
 * Партиционнирование по двум диапозонам, даты до текущего месяца, и после, так как даты отбираются по оператору '<'
 */
 
execute
'DROP TABLE IF EXISTS chirkova.DIM_accounts;
create table chirkova.DIM_accounts(
	account_key         integer not null,
	account_num 		varchar(20) not null,
	valid_to 			date not null,
	client_key 		    integer not null,
	start_dt 		    timestamp  DEFAULT now(),
	end_dt              timestamp  DEFAULT null
)
DISTRIBUTED BY (account_key)

PARTITION BY range (valid_to)
(START (date '''|| min_val_ac||''') INCLUSIVE
    END (date '''|| ed_dt||''') exclusive,
 START (date '''|| ed_dt||''') INCLUSIVE
    END (date '''|| max_val_ac ||''') INCLUSIVE,
 DEFAULT PARTITION other_date);';

/* Создание таблицы измерений 'clients', данные всех колонок данной таблицы не должны принимать значения null
 * Номер мобильного телефона клиента должно начинаться на '+7'
 * В качестве ключа распределения выбран суррогатный ключ client_key, которая принимает уникальные значения
 *  Партиционнирование по двум диапозонам, даты до текущего месяца, и после, так как даты отбираются по оператору '<'
 */

 
execute
'DROP TABLE IF EXISTS chirkova.DIM_clients;
create table chirkova.DIM_clients(
	client_key          integer not null,
	client_id 			varchar(7) not null,
	last_name 			varchar(30) not null,
	first_name 			varchar(30) not null,
	patronymic 			varchar(30) not null,
	date_of_birth 		date not null,
	passport_num 		varchar(10) not null,
	passport_valid_to 	date not null,
	phone 				varchar(15) check ( phone like ''+7%''),
	start_dt 		    timestamp  DEFAULT now(),
	end_dt              timestamp  DEFAULT null
)
DISTRIBUTED BY (client_key)

PARTITION BY range (passport_valid_to)
(START (date '''|| min_val_pas||''') INCLUSIVE
    END (date '''|| ed_dt||''') exclusive,
 START (date '''|| ed_dt||''') INCLUSIVE
    END (date '''|| max_val_pas ||''') INCLUSIVE,
 DEFAULT PARTITION other_date_pas);';


/* Создание таблицы cо следующими методанными по таблицам:
 * Уникальный идентификационный номер таблицы
 * Имя схемы
 * Имя таблицы
 * Имя пользователя, который совершил какое-либо действие над таблицей
 * Соотношение распределения данных между сегментами
 * Дата и время предыдущей очистки
 * Размер таблицы
 * Общее количество строк
 * Последняя операция над таблицей
 * Тип последней операции над таблицей
 * Дата последней операции над таблицей
 */

DROP TABLE IF EXISTS chirkova.META_table;
create table chirkova.META_table(
	table_id 				varchar     not null,
	schemaname 				varchar(30) not null,
	table_name 				varchar     not null,
	usename 				varchar(30) not null,
	part_distribution   	varchar,
	last_vacuum 			timestamp,
	size_t	 				integer not null,
	count_rows				integer not null,
	operation 				varchar,
	type_operation          varchar,
	date_last_operation		timestamp
	
)
DISTRIBUTED BY (table_id);

-- Таблица с отчетом
-- Партиционирование по дням текущего месяца и типу мошенничества
execute
'drop view if exists chirkova.view_report cascade;
DROP TABLE IF EXISTS chirkova.REPORT;
create table chirkova.REPORT(
	FRAUD_DT 				timestamp not null,
	PASSPORT 				varchar not null,
	FIO						varchar(90) not null,
	PHONE 					varchar(15) not null,
	FRAUD_TYPE   			varchar(90),
	REPORT_DT 				timestamp	
)
DISTRIBUTED randomly

PARTITION BY range (FRAUD_DT)
	SUBPARTITION BY LIST (FRAUD_TYPE)
        SUBPARTITION TEMPLATE 
            (
            SUBPARTITION first VALUES (''Совершение операции при просроченном паспорте''),
            SUBPARTITION two VALUES (''Совершение операции при недействующем договоре''),
            SUBPARTITION three VALUES (''Совершение операции в разных городах в течение 1 часа''),
            SUBPARTITION four VALUES (''Попытка подбора сумм'')
            )
(START (date '''|| st_dt||''') INCLUSIVE
    END (date '''|| ed_dt||''') EXCLUSIVE
    EVERY (INTERVAL ''1 day''));';


DROP TABLE IF EXISTS chirkova.META_terminals;
create table chirkova.META_terminals(
	quality_update integer not null,
	quality_insert integer not null,
	dt_update      timestamp not null
)
DISTRIBUTED randomly;

DROP TABLE IF EXISTS chirkova.META_cards;
create table chirkova.META_cards(
	quality_update integer not null,
	quality_insert integer not null,
	dt_update timestamp not null
)
DISTRIBUTED randomly;

DROP TABLE IF EXISTS chirkova.META_accounts;
create table chirkova.META_accounts(
	quality_update integer not null,
	quality_insert integer not null,
	dt_update timestamp not null
)
DISTRIBUTED randomly;

DROP TABLE IF EXISTS chirkova.META_clients;
create table chirkova.META_clients(
	quality_update integer not null,
	quality_insert integer not null,
	dt_update timestamp not null
)
DISTRIBUTED randomly;

return 'Create successfully';

exception
	WHEN OTHERS THEN
        RETURN 'Error ' || SQLSTATE || ' ' || SQLERRM;

END
$$
LANGUAGE plpgsql;

