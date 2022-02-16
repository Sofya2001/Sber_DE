

DROP FUNCTION IF EXISTS chirkova.report();
CREATE OR REPLACE FUNCTION chirkova.report() 
returns void
AS $$
declare
	cur_day date;
begin
-- вычисление текущей даты
	select date(trans_date) into cur_day
			from chirkova.FACT_TRANSACTIONS order by trans_date desc limit 1;


execute

'insert into chirkova.REPORT ( 
with pas_not_valid as (select client_key,passport_num,last_name||'' ''||first_name||'' ''|| patronymic fio, phone 
					   from chirkova.DIM_clients_hist 
					   where passport_valid_to<='''||cur_day||'''),
					   
     d_trans as       (select card_key, trans_date 
					   from chirkova.FACT_TRANSACTIONS 
					   where date(trans_date)='''||cur_day||'''),
					 
     account_cl as     (select passport_num, fio, phone,account_key 
     					from chirkova.DIM_accounts_hist inner join pas_not_valid using(client_key))
     					
	select trans_date,passport_num, fio, phone, ''Совершение операции при просроченном паспорте'' type_fraud, now() from 
            (select trans_date,passport_num, fio, phone,card_key from account_cl cl_ac inner join chirkova.DIM_cards_hist car using(account_key)
            inner join d_trans using (card_key)) q );
					   


insert into chirkova.REPORT (          
with ac_not_valid as       (select account_key, client_key,valid_to from chirkova.DIM_accounts_hist where valid_to<='''||cur_day||'''),
					   
     d_trans as            (select card_key, trans_date  from chirkova.FACT_TRANSACTIONS where date(trans_date)='''||cur_day||'''),
					 
     account_cl as         (select passport_num, last_name||'' ''||first_name||'' ''|| patronymic fio, phone,account_key
     					   from chirkova.DIM_clients_hist inner join ac_not_valid using(client_key))
     					
     select trans_date,passport_num, fio, phone, ''Совершение операции при недействующем договоре'' type_fraud, now() from 
            (select trans_date,passport_num, fio, phone,card_key from account_cl cl_ac inner join chirkova.DIM_cards_hist car using(account_key)
            inner join d_trans using (card_key)) q2);


/*данные группируются в карте, и сортируются по карте и дате, проставляется разница между датами, данные, где разница меньше часа, объеденяются с таблицей терминалов.
Вычисляется количество операций совершенных по карте, и количество операций в в определенном городе(группировака и по карте и по городу),
если общее количество операций больше,чем при группировке с учетом города, то операция была совершена в разных городах.
*/
insert into chirkova.REPORT (  
 with current_hour as       (select card_key, trans_date, terminal_key from(
 								select card_key, trans_date,terminal_key, lead (trans_date) over (partition by card_key order by card_key,trans_date) - trans_date delta,
 								(lag (trans_date) over (partition by card_key order by card_key,trans_date) - trans_date )*(-1) delta_2
 						    from  chirkova.FACT_TRANSACTIONS where trans_date>(select date('''||cur_day||''') - interval ''1 hour'')) a where delta<''1:00:00'' or delta_2<''1:00:00''),
    				
 						
 	  another_city as 		(select card_key, max(trans_date) trans_date from (
 			                 	select card_key, c_t, max(trans_date) trans_date, sum(c_t) c_c from(select card_key, terminal_city, count(terminal_city) c_t, max(trans_date) trans_date from 
 	  								current_hour inner join chirkova.DIM_terminals_hist using(terminal_key) group by card_key,terminal_city) q1 group by card_key, c_t) q2
 	  	               		where c_c>c_t group by card_key)
 		 			
 						
 
 	  select trans_date,passport_num, last_name||'' ''||first_name||'' ''|| patronymic fio, phone, ''Совершение операции в разных городах в течение 1 часа'' type_fraud, now() from(
      select account_key,client_key, trans_date from another_city  inner join chirkova.DIM_cards_hist using(card_key) inner join chirkova.DIM_accounts_hist using(account_key)) j inner join
      			chirkova.DIM_clients_hist using(client_key)) ;

 /*данные группируются в карте, и сортируются по карте и дате, проставляется разница между датами и вычисляется общая разница, если она меньше 20 минут,
то данным проставляется ранги по дате, сумме(amt), и результату операции, если ранги совпадают, при этом только у последней строки, ранг результат операции равно 2, то считается 
количество таких записей, если оно совпадает с изначальным количеством выбранных строк, то это искомые строки, которые далее джоинятся и извлекается необходимые для отчета данные
*/    		
insert into chirkova.REPORT (
with delta_time_count as ( select card_key, trans_date, delta,delta_2,c_t, amt,oper_result,
								sum(delta) over  (partition by card_key order by card_key,trans_date) cum_sum,
								DENSE_RANK () OVER ( partition by card_key ORDER BY trans_date ) r_date,
								DENSE_RANK () OVER ( partition by card_key ORDER BY amt desc) r_amt,
								DENSE_RANK () OVER ( partition by card_key ORDER BY oper_result) r_result
							from(
 									select card_key,amt, oper_result,trans_date,terminal_key, lead (trans_date) over (partition by card_key order by card_key,trans_date) - trans_date delta,
 										  (lag (trans_date) over (partition by card_key order by card_key,trans_date) - trans_date )*(-1) delta_2,
 										   count(trans_date) over (partition by card_key order by card_key) c_t
 									from  chirkova.FACT_TRANSACTIONS where trans_date>(select date('''||cur_day||''') - interval ''20 minute'')) a
								    where (delta<''00:20:00'' or delta_2<''00:20:00'') and c_t>3 ),
	  chek as  ( select card_key, trans_date, delta,delta_2,c_t, amt,oper_result, cum_sum,r_date, r_amt,	r_result 
						   from delta_time_count
 						   where cum_sum<''00:20:00'' and (c_t!=r_date and r_date=r_amt and r_result=1) or (c_t=r_date and r_date=r_amt and r_result=2)),
 						   
      result_num as  ( select card_key,max(trans_date) trans_date from chek 
                			where c_t=(select distinct count(trans_date) over (partition by card_key order by card_key) from chek) group by card_key)
                			
                			
 	  select trans_date,passport_num, last_name||'' ''||first_name||'' ''|| patronymic fio, phone, ''Попытка подбора сумм'' type_fraud, now() from(
     	 select client_key,trans_date from (
     		 select account_key, trans_date from result_num  inner join chirkova.DIM_cards_hist using(card_key)) a_n inner join chirkova.DIM_accounts_hist using(account_key)) d inner join
      			chirkova.DIM_clients_hist using(client_key));

	drop view if exists chirkova.view_report cascade;
	CREATE VIEW chirkova.view_report AS SELECT * from chirkova.report where date(fraud_dt)='''||cur_day||''';';
      		
  
END
$$
LANGUAGE plpgsql;


