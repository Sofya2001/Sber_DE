
/*Создание внешней таблицы, которая обеспечивает доступ к данным в источниках за пределами Greenplum, для загрузки данных используем утилиту gpfdist.
 * Извлечение данных и их загрузка во внешнюю таблицу:
 * Команда,которая была прописана для запуска утилиты : gpfdist -d /home/gpadmin/Sber_DE-main/ -p 8001  &
 * В параметре location указываем местроспаложение файла: ip сервера - 192.168.43.199, порт GPFDist - 8001, название искомого файла.
 * В параметре format указываем разрешение файла, с которого будут считываться данные, в данном случае это формат csv c разделителем ',' с игнорированием первой строки
 * Создание временной таблицы, и загрузка в нее данных из внешней таблицы
 */
DROP FUNCTION IF EXISTS chirkova.load_data(ip varchar,port varchar, file varchar);
CREATE OR REPLACE FUNCTION chirkova.load_data(ip varchar,port varchar, file varchar) 
RETURNS text
AS $$
declare 
	query text;
	n integer;
begin
	query := 'DROP external TABLE IF EXISTS chirkova.SRS_data;
	create external table   chirkova.SRS_data(
		trans_id 			varchar(9),
		trans_date  		timestamp,
		card_num 			varchar(20),
		account_num 		varchar(20),
		valid_to 			date,
		client 				varchar(7),
		last_name 			varchar(30),
		first_name 			varchar(30),
		patronymic 			varchar(30),
		date_of_birth 		date,
		passport_num 		varchar(10),
		passport_valid_to 	date,
		phone 				varchar(15),
		oper_type 			varchar(20),
		amt 				decimal,
		oper_result 		varchar(20),
		terminal 			varchar(8),
		terminal_type 		varchar(3),
		terminal_city 		varchar(30),
		terminal_address 	varchar(60)
	)
	location (''gpfdist://'||ip||':'||port||'/'||file||''')
	FORMAT ''CSV'' ( delimiter '','' HEADER);';
	execute query;

	DROP TABLE if exists STG_data;
	CREATE TEMPORARY TABLE STG_data ( like chirkova.SRS_data )
	DISTRIBUTED BY (trans_id);

	INSERT INTO STG_data
	SELECT * FROM chirkova.SRS_data;


	return 'Successfully';
exception
	WHEN OTHERS THEN
        RETURN 'error ' || SQLSTATE || ' ' || SQLERRM;
END
$$
LANGUAGE plpgsql;



--Процедура по загрузке данных в хранимые таблицы
DROP FUNCTION IF EXISTS chirkova.load_data_in_norm_structure();
CREATE OR REPLACE FUNCTION chirkova.load_data_in_norm_structure()
RETURNS text
AS $$
declare 
	cur_day             integer;
	count_row			integer;
	count_not_duplicat 	integer;
	crs_id 				varchar;
	crs_type 			varchar;
	crs_city 			varchar;
	crs_address 		varchar;
	quantity_up         integer :=0;
	quantity_in         integer :=0;
	crs_terminal CURSOR FOR (select   terminal, terminal_type, terminal_city, terminal_address
							 from   STG_data 
							 group by terminal, terminal_type, terminal_city, terminal_address)
							 except 
							(select   terminal_id, terminal_type, terminal_city, terminal_address
							 from      chirkova.DIM_terminals_hist
							 group by  terminal_id, terminal_type, terminal_city, terminal_address);
	crs_card_num        varchar;
	crs_dic CURSOR FOR    (select card_num 
							 from STG_data
							 group by card_num)	
							 except
							 (select card_num	 
							 from chirkova.dim_cards_directory
							 group by card_num);
													
	crs_card			integer;
	crs_account  		integer;
	cur_card_c            varchar;
	cur_account         varchar;
	crs_cards CURSOR FOR    (select card_dir_key, account_key	 
							 from STG_data inner join chirkova.dim_cards_directory using(card_num) inner join 
							 	(select account_key,account_num from chirkova.DIM_accounts_hist where end_dt is null) a using(account_num)
							 group by card_dir_key, account_key)	
							 except
							 (select card_dir_key, account_key	 
							 from chirkova.DIM_cards_hist
							 group by card_dir_key, account_key)
							  ;
							
	crs_account_num		varchar;
	crs_valid_to_a  	date;
	crs_client_key		integer;
	cl_id               varchar;
	crs_accounts CURSOR FOR  (select  account_num, valid_to, client_key	
							  from STG_data inner join (select client_key,client_id from chirkova.DIM_clients_hist where end_dt is null) a on client=client_id
							  group by account_num, valid_to, client_key)			
							  except 	
							  (select account_num, valid_to, client_key	
							  from chirkova.DIM_accounts_hist
							  group by account_num, valid_to, client_key);
			
	crs_client      	varchar;
	crs_last_name  		varchar;
	crs_first_name		varchar;
	crs_patronymic      varchar;
	crs_birth  		    date;
	crs_passport_num	varchar;
	crs_valid_to 	    date;
	crs_phone			varchar;
	crs_clients CURSOR FOR (select   client, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone
							from     STG_data 
							group by client, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone)
							except 
						   (select  client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone
							from     chirkova.DIM_clients_hist		
							group by client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone);
						
	tr_cr_num           varchar;
	tr_ter              varchar;
	crs_trans_id		varchar;
	cur_card            integer;
	cur_ter             integer;
	crs_trans CURSOR FOR   (select   trans_id 	  	
							from     STG_data 
							group by trans_id)
							except 
							(select   trans_id 	  	
							from      chirkova.FACT_TRANSACTIONS
							group by trans_id);
						
	id 					varchar;
	scheman 			varchar;
	r_n 				varchar;
	u_n 				varchar;
	l_v 				timestamp;
	part_dis 			varchar;
	collumn 			integer;
	size_f 				integer;
	count_row_m 		integer;
	oper      			varchar;
	t_oper 				varchar;
	date_oper 			timestamp;
	c_r     			integer;
	crs_name 			varchar;
	crs_check CURSOR for select 
								relname
						from gp_dist_random('pg_stat_all_tables') 
						where schemaname='chirkova' and 
							  relname like 'dim_%' or 
							  relname like 'fact_%' 
						group by relname;
	

begin
/*Проверка на начало месяца, если данные поступают первого числа, то таблицы пересоздаются
 */
	select date_part('day',(select to_timestamp(to_char(trans_date,'YYYY-DD-MM HH24:MI:SS'),'YYYY-MM-DD HH24:MI:SS'))) into cur_day
			from (select trans_date from STG_data order by trans_date desc limit 1) a ;	
	if cur_day=1 then
		 perform chirkova.create_table();
	end if;

/* Загрузка данных из временной таблицы в таблицу(SCD2) с даннымм о терминалах, при попытке вставки идентичной записи, запись не вставляется, 
 * при попытке вставки строки с идентичным идентификатором, при этом какие-либо данные данной строки изменены,запись вставляется в таблицу, с присваением
 * суррогатного ключа, в старую версию строки в дополнительный столбец с датой конца версии простовляется дата начала новой версии строки.
 * Загрузка данных из временной таблицы в таблицу(SCD1) с даннымм о терминалах, при попытке вставки идентичной записи, запись не вставляется, 
 * при попытке вставки строки с идентичным идентификатором, запись обновляется
 */
	OPEN crs_terminal;
		loop
			FETCH crs_terminal INTO crs_id, crs_type, crs_city,crs_address;
			IF NOT FOUND THEN EXIT;END IF;
			select count(1) into count_row from chirkova.DIM_terminals_hist where 
				terminal_id=crs_id and end_dt is null;
			if count_row>0 then 
				insert into chirkova.DIM_terminals_hist values(nextval('chirkova.terminals_seq'),crs_id, crs_type, crs_city,crs_address);
				update chirkova.DIM_terminals_hist set end_dt=(select start_dt from chirkova.DIM_terminals_hist
					where terminal_id=crs_id and terminal_type=crs_type and terminal_city=crs_city and terminal_address=crs_address) 
					where terminal_id=crs_id and (terminal_type!=crs_type or terminal_city!=crs_city or terminal_address!=crs_address) and 
						end_dt is null;
				update chirkova.DIM_terminals set terminal_type=crs_type,terminal_city=crs_city, terminal_address=crs_address, update_dt=(select now())
					where terminal_id=crs_id and (terminal_type!=crs_type or terminal_city!=crs_city or terminal_address!=crs_address);
				quantity_up:=quantity_up+1;
	        else
				insert into chirkova.DIM_terminals_hist values(nextval('chirkova.terminals_seq'),crs_id, crs_type, crs_city,crs_address);
				insert into chirkova.DIM_terminals values(crs_id, crs_type, crs_city,crs_address);
				quantity_in:=quantity_in+1;
			end if;
		END LOOP;
	CLOSE crs_terminal;
	insert into chirkova.META_terminals values(quantity_up,quantity_in,now());
	quantity_up:=0;
	quantity_in:=0;


/* Справочник с номерами карт, осуществляется проверка на наличие данного номера в справочника, если номера нет, то номер вставляется и записи присваевается
 * суррогатный ключ.
 */
	OPEN crs_dic;
		loop
			FETCH crs_dic INTO crs_card_num;
			IF NOT FOUND THEN EXIT;END IF;
			select count(1) into count_row from chirkova.dim_cards_directory where 
				card_num=crs_card_num;
			if count_row=0 then 
				insert into chirkova.dim_cards_directory values(nextval('chirkova.dim_accounts_seq'),crs_card_num);
			end if;
		END LOOP;
	CLOSE crs_dic;




/* Загрузка данных из временной таблицы в таблицу(SCD2) с данными о клиентах, при попытке вставки идентичной записи, запись не вставляется, 
 * при попытке вставки строки с идентичным идентификатором, при этом какие-либо данные данной строки изменены,запись вставляется в таблицу,с присваением
 * суррогатного ключа, в старую версию строки в дополнительный столбец с датой конца версии простовляется дата начала новой версии строки.
 * Загрузка данных из временной таблицы в таблицу(SCD1) с даннымм о клиентах, при попытке вставки идентичной записи, запись не вставляется, 
 * при попытке вставки строки с идентичным идентификатором, запись обновляется
 */	
	OPEN crs_clients;
		loop
			FETCH crs_clients INTO crs_client, crs_last_name , crs_first_name, crs_patronymic, crs_birth , crs_passport_num, crs_valid_to, crs_phone;
			IF NOT FOUND THEN EXIT;END IF;
			select count(1) into count_row from chirkova.DIM_clients_hist where 
					crs_client=client_id and end_dt is null;
			if count_row>0 then 
				insert into chirkova.DIM_clients_hist values(nextval('chirkova.dim_clients_seq'),crs_client, crs_last_name , crs_first_name, crs_patronymic, crs_birth , crs_passport_num, crs_valid_to, crs_phone);
				update chirkova.DIM_clients_hist set end_dt=(select start_dt from chirkova.DIM_clients_hist
					where crs_client=client_id and  crs_last_name=last_name and crs_first_name=first_name and crs_patronymic=patronymic and crs_passport_num=passport_num and crs_valid_to=passport_valid_to and crs_phone=phone) 
					where crs_client=client_id and (crs_last_name!=last_name or crs_first_name!=first_name or  crs_patronymic!=patronymic or crs_passport_num!=passport_num or crs_valid_to!=passport_valid_to or crs_phone!=phone)
						and end_dt is null;
				update chirkova.DIM_clients set last_name=crs_last_name, first_name=crs_first_name, patronymic=crs_patronymic,passport_num=crs_passport_num, passport_valid_to=crs_valid_to, phone=crs_phone, update_dt=(select now())
					where crs_client=client_id and (crs_last_name!=last_name or crs_first_name!=first_name or  crs_patronymic!=patronymic or crs_passport_num!=passport_num or crs_valid_to!=passport_valid_to or crs_phone!=phone);
				quantity_up:=quantity_up+1;
			else
				insert into chirkova.DIM_clients_hist values(nextval('chirkova.dim_clients_seq'),crs_client, crs_last_name , crs_first_name, crs_patronymic, crs_birth , crs_passport_num, crs_valid_to, crs_phone);
				insert into chirkova.DIM_clients values(crs_client, crs_last_name , crs_first_name, crs_patronymic, crs_birth , crs_passport_num, crs_valid_to, crs_phone);
				quantity_in=quantity_in+1;
			end if;
		END LOOP;
	CLOSE crs_clients;
	insert into chirkova.META_clients values(quantity_up,quantity_in,now());
	quantity_up:=0;
	quantity_in:=0;

/* Загрузка данных из временной таблицы в SCD2, к записям которым были присоеденены суррогатные ключи клиентов из таблицы с клиентами, при попытке вставки записи, где ключ клиента или дата окончания договора различны,
 * а номера счетов идентичны, то запись с новыми данными вставляется, в старую версию строки в дополнительный столбец с датой конца версии проставляется дата начала новой версии записи.
 * Загрузка данных из временной таблицы в таблицу(SCD1) с даннымм о счетах, при попытке вставки идентичной записи, запись не вставляется, 
 * при попытке вставки строки с идентичным идентификатором, запись обновляется
 */	

	OPEN crs_accounts;
		loop
			FETCH crs_accounts INTO crs_account_num, crs_valid_to_a, crs_client_key;
			IF NOT FOUND THEN EXIT;END IF;
			select client into cl_id from stg_data where account_num=crs_account_num;
			select count(1) into count_row from chirkova.DIM_accounts_hist where 
				crs_account_num=account_num and end_dt is null ;
			if count_row>0 then 
				insert into chirkova.DIM_accounts_hist values(nextval('chirkova.dim_accounts_seq'),crs_account_num, crs_valid_to_a, crs_client_key);
				update chirkova.DIM_accounts_hist set end_dt=(select start_dt from chirkova.DIM_accounts_hist
					where crs_account_num=account_num and crs_valid_to_a=valid_to and crs_client_key=client_key) 
					where crs_account_num=account_num and (crs_valid_to_a!=valid_to or crs_client_key!=client_key) and end_dt is null;
				update chirkova.DIM_accounts set  valid_to=crs_valid_to_a , client_id=cl_id
					where crs_account_num=account_num and (crs_valid_to_a!=valid_to or client_id!=cl_id) ;
				quantity_up=quantity_up+1;
			else
				insert into chirkova.DIM_accounts_hist values(nextval('chirkova.dim_accounts_seq'),crs_account_num, crs_valid_to_a, crs_client_key);
			 	insert into chirkova.DIM_accounts values(crs_account_num, crs_valid_to_a, cl_id);
				quantity_in=quantity_in+1;
			end if;
		END LOOP;
	CLOSE crs_accounts;
	insert into chirkova.meta_accounts values(quantity_up,quantity_in,now());
	quantity_up:=0;
	quantity_in:=0;

/* Загрузка данных из временной таблицы в SCD2, к записям которым были присоеденены суррогатные ключи счетов из таблицы со счетами и номерами карт из справочника, при попытке вставки записи, где ключ карты идентичен,
 *  то запись с новыми данными вставляется, в старую версию строки в дополнительный столбец с датой конца версии проставляется дата начала новой версии записи.
 *Загрузка данных из временной таблицы в таблицу(SCD1) с даннымм о картах, при попытке вставки идентичной записи, запись не вставляется, 
 * при попытке вставки строки с идентичным идентификатором, запись обновляется
  */	



	OPEN crs_cards;
		loop
			FETCH crs_cards INTO crs_card, crs_account;
			IF NOT FOUND THEN EXIT;END IF;
			select card_num  into cur_card_c from chirkova.dim_cards_directory where card_dir_key=crs_card;
			select account_num  into cur_account from chirkova.DIM_accounts_hist where account_key=crs_account;
			select count(1) into count_row from chirkova.DIM_cards_hist where 
				card_dir_key=crs_card and end_dt is null;
			if count_row>0 then 
				insert into chirkova.DIM_cards_hist values(nextval('chirkova.dim_cards_seq'),crs_card, crs_account);
				update chirkova.DIM_cards_hist set end_dt=(select start_dt from chirkova.DIM_cards_hist
					where card_dir_key=crs_card and account_key=crs_account) 
					where  card_dir_key=crs_card and account_key!=crs_account and end_dt is null;
				update chirkova.DIM_cards set account_num=cur_account
					where  cur_card=card_num and account_num!=cur_account;
				quantity_up=quantity_up+1;
			else
				insert into chirkova.DIM_cards_hist values(nextval('chirkova.dim_cards_seq'),crs_card,crs_account);
				insert into chirkova.DIM_cards values(cur_card_c,cur_account);
				quantity_in=quantity_in+1;
			end if;
		END LOOP;
	CLOSE crs_cards;
	insert into chirkova.meta_cards values(quantity_up,quantity_in,now());
	quantity_up:=0;
	quantity_in:=0;



/*Вставка данных в таблицы фактов, где содержится информация о транзакциях в формате SCD2 и SCD1, при выгрузке данных из файла была проблема с формированием даты, вместо даты был месяц и наоборот,
 соответственно для решения данной проблемы формат даты был изменен, далее для того,чтобы субд верно считывала данные, дата преобразована в строку,а далее снова в timestamp
 Вставка актуального ключа номера и терминала вместо номеров карт и терминалов соотвественно
 */
	OPEN crs_trans;
		loop
			FETCH crs_trans INTO crs_trans_id;
			IF NOT FOUND THEN EXIT;END IF;
			select card_num, terminal into tr_cr_num,tr_ter
									from STG_data	
									where trans_id=crs_trans_id;
			 execute 'select card_key from chirkova.dim_cards_hist inner join chirkova.dim_cards_directory using(card_dir_key) 
									where end_dt is null and card_num='''||tr_cr_num||'''' into cur_card;
			 execute 'select terminal_key from chirkova.dim_terminals_hist
									where end_dt is null and terminal_id='''||tr_ter||'''' into cur_ter;
									
			 execute 'insert into chirkova.FACT_TRANSACTIONS
									(select trans_id, to_timestamp(to_char(trans_date,''YYYY-DD-MM HH24:MI:SS''),''YYYY-MM-DD HH24:MI:SS'') trans_date,'||cur_card||' card_key, oper_type,
										   amt, oper_result,'|| cur_ter||' terminal_key
									from STG_data	
									where trans_id='||crs_trans_id|| '
									group by trans_id, trans_date, card_key, oper_type, amt, oper_result, terminal_key);';	
								
			 execute 'insert into chirkova.FACT_TRANSACTIONS_FOR_SCD1
									(select trans_id, to_timestamp(to_char(trans_date,''YYYY-DD-MM HH24:MI:SS''),''YYYY-MM-DD HH24:MI:SS'') trans_date,card_num, oper_type,
										   amt, oper_result,terminal
									from STG_data	
									where trans_id='||crs_trans_id|| '
									group by trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal);';
		end loop;
	close crs_trans;


								
/*Вставка методанных о таблицах, курсов пробегается по словарю с названием таблиц в схеме chirkova, далее из системных вьюшек достает необходимые данные о текущей в курсоре таблице,
 * если таблица существует в таблице с методанными, то данные таблицы  обновляются, если  нет, то вставляются*/							
								
	OPEN crs_check;
		LOOP
			FETCH crs_check INTO crs_name;
			IF NOT FOUND THEN EXIT;END IF;
	    	select distinct relid,schemaname,relname,usename,last_vacuum into id,scheman,r_n,u_n,l_v from pg_catalog.pg_stat_operations o inner join (
					   select relid,relname,last_vacuum from gp_dist_random('pg_stat_all_tables') where relname= crs_name) t on o.objid=t.relid;
			execute 'select party_seg from (select(num_rows || '' : '' || lead (num_rows) OVER (  ORDER BY  gp_segment_id)) as party_seg FROM
									(select gp_segment_id, COUNT(1) AS num_rows FROM chirkova.'|| crs_name ||' GROUP BY gp_segment_id ORDER BY  gp_segment_id) as b ) as a where party_seg is not null;'
					into part_dis;
			execute 'select count(1) from chirkova.'||crs_name||';' into count_row_m;
			select sotdsize into size_f from gp_toolkit.gp_size_of_table_disk where sotdtablename =crs_name;
			select actionname, subtype,statime into oper,t_oper,date_oper 
				from pg_catalog.pg_stat_operations where objname=crs_name and statime=(
				select max(statime) from pg_catalog.pg_stat_operations where objname=crs_name);
			select count(1) into c_r from chirkova.META_table where table_name=crs_name;
			if c_r>0  then
				update chirkova.META_table set part_distribution=part_dis,last_vacuum=l_v,size_t=size_f,count_rows=count_row_m,date_last_operation=date_oper,operation=oper,type_operation=t_oper
				where table_name=crs_name;
			else
				insert into chirkova.META_table values (id,scheman,r_n,u_n,part_dis,l_v,size_f,count_row_m,oper,t_oper,date_oper);
			end if;
	  	END LOOP;
	 CLOSE crs_check;
	-- Сбор статистики 
	OPEN crs_check;
			LOOP
				FETCH crs_check INTO crs_name;
				IF NOT FOUND THEN EXIT;END IF;
				execute 'analyze chirkova.'||crs_name||';';
			end loop;
		 CLOSE crs_check;
		
	return 'Load successfully';			
exception
	WHEN OTHERS THEN
        RETURN 'Error ' || SQLSTATE || ' ' || SQLERRM;
END
$$
LANGUAGE plpgsql;
