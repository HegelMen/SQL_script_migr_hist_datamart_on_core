-- ЗАДАЧА 2770 "ПЕРЕНОС ИСТОРИИ"
--СКРИПТ ДЛЯ public.t_metric_empl_day


-- 1. СТАРТ ФАЗА PIPELINE - СОБИРАЕМ ДАТАСЕТ ИЗ старой ВИТРИНЫ "public.t_metric_empl_day"
drop table if exists tmp_dataset_1_ssms_2770;
create temporary table tmp_dataset_1_ssms_2770 as                                                    --исходный датасет - сборка за весь период
	select 
		tmp0.dtdate,		
		tmp0.emplid,
		tmp0.deptcd,		
		unnest(array[tmp0.productivity_empl, tmp0.workinghours, tmp0.cnt_timesheets, tmp0.sale_smartphones_on,
				 tmp0.sale_smartphones_off, tmp0.cnt_calc_tariffs_30sec, tmp0.cnt_calc_devices_30sec,
				 tmp0.cnt_calc_tariffs, tmp0.cnt_calc_devices, tmp0.sale_tariffs, tmp0.sup,
				 tmp0.productivity_empl_plan, tmp0.workinghours_without_dinner, tmp0.workinghours_plan,
				 tmp0.share_of_normal_office_productivity, tmp0.cnt_timesheets_of_new_employees, tmp0.workinghours_plan_without_dinner,
				 tmp0.workinghours_overtime, tmp0.percent_overtime, tmp0.cnt_calc_tariffs_106sec,
				 tmp0.cnt_calc_devices_106sec, tmp0.cnt_calc_devices_changed_interviews, tmp0.cnt_calc_tariffs_changed_interviews,
				 tmp0.traffic_empl, tmp0.cnt_conf_devices_with_refuse]
			   ) as val,
		unnest(array['productivity_empl', 'workinghours', 'cnt_timesheets', 'sale_smartphones_on', 
	             'sale_smartphones_off', 'cnt_calc_tariffs_30sec', 'cnt_calc_devices_30sec',
				 'cnt_calc_tariffs', 'cnt_calc_devices', 'sale_tariffs', 'sup',
				 'productivity_empl_plan', 'workinghours_without_dinner', 'workinghours_plan',
				 'share_of_normal_office_productivity', 'cnt_timesheets_of_new_employees',
				 'workinghours_plan_without_dinner', 'workinghours_overtime', 'percent_overtime',
				 'cnt_calc_tariffs_106sec', 'cnt_calc_devices_106sec', 'cnt_calc_devices_changed_interviews', 
				 'cnt_calc_tariffs_changed_interviews', 'traffic_empl, cnt_conf_devices_with_refuse']
		       ) as col 
	from public.t_metric_empl_day as tmp0
	--where ym =2101
;


--2 ФАЗА PIPELINE. ОЧИЩАЕМ ДАТАСЕТ
drop table if exists tmp_dataset_2_ssms_2770;
create temporary table tmp_dataset_2_ssms_2770 as
	select 				
		tmp1.dtdate                  as report_dt,  
		tmp1.emplid, 	             --as entity_emp_id,   
		tmp1.deptcd,	 	
		tmp1.val                     as metric_value,
		tmp1.col			
	from tmp_dataset_1_ssms_2770 as tmp1		
	where tmp1.val is not null -- and val != 0                                          -- удаляем метрики с нулевым и null знач.		
	  and tmp1.dtdate >= '2022-01-01' and tmp1.dtdate <= '2022-12-31'                   -- удаляем записи по фильтру расчетного периода метрики 
;                 

-- 3.1. ОБОГАЩАЕМ ДАТАСЕТ (уже СУЩЕСТВУЮЩИМИ ключами Измерений и Линков). 

drop table if exists tmp_dataset_3_1_ssms_2770;
create temporary table tmp_dataset_3_1_ssms_2770 as
	select
		tmp2.report_dt, 
		deh.emp_id                                          as entity_emp_id,		
		tmp2.metric_value,
		dd.department_id                                    as entity_department_id,    -- добавляем ключ измерения entity_department_id 		
		case 
			when tmp2.col like '%plan%' then 1  			
			else 2
		end                                                 as metric_type_id,			-- добавляем ключ измерения типа метрики metric_type_id 	
		dm.metric_id                                        as metric_id,               -- добавляем ключ измерения metric_id
		mac.metric_attr_set_id                              as metric_attr_set_id	    -- добавляем ключ линка связки наборов metric_attr_set_id
	from tmp_dataset_2_ssms_2770 tmp2
    left join core.dim_department as dd  on tmp2.deptcd = dd.department_short_name   
    left join (                                                                              
    			select *
				from (
					  select emp_id, tab_num, emp_status_start_dt, emp_status_end_dt, 
					  row_number() over (partition by emp_id, emp_status_end_dt) as row_num
					  from core.dm_emp_hist					  
				) t1
				where row_num = 1
			   ) as deh   	   
    		on deh.tab_num::text = tmp2.emplid::text    	  
    	   and (tmp2.report_dt between deh.emp_status_start_dt and deh.emp_status_end_dt)  
    left join (select * from core.dic_metric where metric_id not in (25, 26)) as dm on tmp2.col = dm.metric_name     -- костыль 'джойна на str'  есть дубли name в справочнике			    
	left join core.t_metric_attr_set mac on                                                                                             
			(deh.emp_id = mac.entity_emp_id and dd.department_id = mac.entity_department_id) or 
			(deh.emp_id = mac.entity_emp_id and dd.department_id is null and mac.entity_department_id = -1)  -- !!!не только -1 в t_metric_attr_set, но и null в справочнике !!!! (иначе дубли) 
	where deh.emp_id  is not null                                                        -- ИСКЛЮЧАЕМ null emp_id из-за связки ТАБЕЛЬНИКОВ::text
	  and metric_id is not null                                                          -- ИСКЛЮЧАЕМ null metric_id из-за связки на костыль дублей metric_name

;

--3.2. ВЫДЕЛЯЕМ ИЗ ДАТАСЕТА НОВЫЕ наборы id для ИЗМЕРЕНИЙ и ЛИНКОВ
with cte_dataset_3_2_metric_attr_set_ssms_277 as                                         -- CTE с данными для заливки в линк - metric_attr_set           
	(select distinct		
		case
			when (tmp3_1.entity_emp_id is null) and (tmp3_1.entity_department_id is not null)  then '-1'::integer                                       			
			else tmp3_1.entity_emp_id
		end                                                                as entity_emp_id,
		case 
			when tmp3_1.entity_department_id is null and (tmp3_1.entity_emp_id is not null) then '-1'::integer                                       			
			else tmp3_1.entity_department_id
		end                                                                as entity_department_id				
	from tmp_dataset_3_1_ssms_2770 as tmp3_1
	where metric_attr_set_id is null 
	)	
--3.3. ПРОЛИВАЕМ новыми данными ИЗМЕРЕНИЯ и ЛИНКИ
	-- ЛИНК t_metric_attr_set      
	insert into core.t_metric_attr_set (entity_emp_id, entity_department_id)                                                                                   
	select entity_emp_id, entity_department_id 		
	from cte_dataset_3_2_metric_attr_set_ssms_277
	where entity_emp_id is not null and entity_department_id is not null                             
	on conflict (entity_emp_id, entity_department_id, main_position_id, 
		         nfs_position_id, nfs_post_id, up_category_id)
	do update 
		  set
			entity_emp_id = excluded.entity_emp_id,
			entity_department_id=excluded.entity_department_id,
			main_position_id = excluded.main_position_id,
			nfs_position_id = excluded.nfs_position_id,
			nfs_post_id = excluded.nfs_post_id,
		    up_category_id = excluded.up_category_id
;

--3.4. ОБОГАЩАЕМ ПОВТОРНО ДАТАСЕТ (вновь ДОБАВЛЕННЫМИ ключами Измерений и Линков) 
drop table if exists tmp_dataset_3_4_ssms_2770;
create temporary table tmp_dataset_3_4_ssms_2770 as
	select
		tmp2.report_dt, 	
		deh.emp_id                                          as entity_emp_id,	
		tmp2.metric_value,
		dd.department_id                                    as entity_department_id,    -- добавляем ключ измерения entity_department_id 		
		case 
			when tmp2.col like '%plan%' then 1  			
			else 2
		end                                                 as metric_type_id,			-- добавляем ключ измерения типа метрики metric_type_id 	
		dm.metric_id                                        as metric_id,               -- добавляем ключ измерения metric_id
		mac.metric_attr_set_id                              as metric_attr_set_id     -- добавляем ключ линка связки наборов metric_attr_set_id		
	from tmp_dataset_2_ssms_2770 tmp2
    left join core.dim_department as dd  on tmp2.deptcd = dd.department_short_name   
    left join (                                                                              
    			select *
				from (
					  select emp_id, tab_num, emp_status_start_dt, emp_status_end_dt, 
					  row_number() over (partition by emp_id, emp_status_end_dt) as row_num
					  from core.dm_emp_hist					
				) t1
				where row_num = 1
			   ) as deh   	   
    		on deh.tab_num::text = tmp2.emplid::text    	  
    	   and (tmp2.report_dt between deh.emp_status_start_dt and deh.emp_status_end_dt) 
    left join (select * from core.dic_metric where metric_id not in (25, 26)) as dm on tmp2.col = dm.metric_name     -- костыль 'джойна на str'  есть дубли name в справочнике			    
	left join core.t_metric_attr_set mac on                                                                                             
			(deh.emp_id = mac.entity_emp_id and dd.department_id = mac.entity_department_id) or 
			(deh.emp_id = mac.entity_emp_id and dd.department_id is null and mac.entity_department_id = -1)  -- !!!не только -1 в t_metric_attr_set, но и null в справочнике !!!! (иначе дубли) 
	where deh.emp_id  is not null                                                        -- ИСКЛЮЧАЕМ null emp_id (emp нет в справочнике или из-за связки ТАБЕЛЬНИКОВ::text)
	  and metric_id is not null                                                          -- ИСКЛЮЧАЕМ null metric_id из-за связки на костыль дублей metric_name

;

  
--4. ИТОГ ФАЗА PIPELINE. ЗАЛИВКА в ФАКТЫ результирующего набора 
-- Пост ККД. Убрать дубли строк 
with data_for_insert as 
	(select
	 	t2.metric_id, 
		t2.report_dt, 
		t2.metric_type_id,                                             
		t2.metric_attr_set_id, 
		t2.metric_value
	 from (
		select   
				t1.metric_id, 
				t1.report_dt, 
				t1.metric_type_id,                                             
				t1.metric_attr_set_id, 
				t1.metric_value,
				row_number() over (partition by metric_id, report_dt, metric_type_id, metric_attr_set_id, metric_value) rwn
		from tmp_dataset_3_4_ssms_2770 t1
	  ) t2	
	  where rwn = 1
)
--Инсерт НОВЫХ записей 
insert into core.t_metric(
	metric_id,
	report_dt,
	metric_type_id, 
	metric_attr_set_id,                         
	metric_value) 
	select   
		t3.metric_id, 
		t3.report_dt, 
		t3.metric_type_id,                                             
		t3.metric_attr_set_id, 
		t3.metric_value 
	from data_for_insert t3
	left join core.t_metric t4 on t3.metric_id = t4.metric_id                    -- т.к. нет pk в t_metric
                              and t3.report_dt = t4.report_dt 
                              and t3.metric_type_id = t4.metric_type_id 
                              and t3.metric_attr_set_id = t4.metric_attr_set_id 
                              and t3.metric_value = t4.metric_value
	where t4.metric_id is null	 
;
