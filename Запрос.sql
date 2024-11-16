select q6.mop_id,
	   q6.name_mop,
	   q7.rop_name,
	   round(extract(hour from q5.avg_time_diff) * 60 + extract(minute from q5.avg_time_diff) + extract(second from q5.avg_time_diff) / 60, 1) as avg_minutes_for_answer
from(
	select q4.created_by, avg(q4.time_diff) as avg_time_diff
	from (select *
		  from (select q2.message_id,
	                   q2.type,
	                   q2.entity_id,
	                   q2.created_by,
	                   q2.created_at_datetime,
	                   q2.prev_datetime,
	                   (case 
	   	                	when (q2.prev_datetime::time between '00:00:00' and '09:30:00') and q2.created_at_datetime::time >= '09:30:00' then 
             					q2.created_at_datetime::time - '09:30:00'
    						when q2.prev_datetime < date_trunc('day', q2.created_at_datetime) and q2.created_at_datetime >= date_trunc('day', q2.created_at_datetime) + interval '09:30:00' then 
    							(q2.created_at_datetime - q2.prev_datetime - interval '09:30:00')
    						else
        						(q2.created_at_datetime - q2.prev_datetime)
	                    end) as time_diff
                from (select q1.message_id,
	                          q1.type,
	                          q1.entity_id,
	                          q1.created_by,
	                          q1.created_at_datetime,
	                          (case 
	   	                       		when q1.type != lag(q1.type) over (partition by q1.entity_id order by q1.created_at_datetime) and q1.type = 'outgoing_chat_message' then 
	   		                        	lag(q1.created_at_datetime) over (partition by q1.entity_id order by q1.created_at_datetime) 
	   	                            else
	   		                        	null
	                           end) as prev_datetime
                      from (select message_id,
       	                            (case 
       	   		                     	when created_by = 0 then
       	   			                    	'incoming_chat_message'
       	   	                            else 
       	   		                     		type
       	                             end) as type,
                                     entity_id,
                                     created_by,
                                     to_timestamp(created_at) AS created_at_datetime
                             from test.chat_messages) q1
                      order by q1.entity_id, q1.created_at_datetime) q2) q3
          where (q3.created_at_datetime::time not between '00:00:00' and '09:30:00')
		      and (q3.type = 'outgoing_chat_message')
		  	  and (q3.time_diff is not null)) q4
	group by q4.created_by
	order by avg_time_diff) q5
right join (select mop_id, name_mop, cast(rop_id as integer) as rop_id
			from test.managers) q6
on q5.created_by = q6.mop_id
join (select *
	  from test.rops) q7
on q6.rop_id = q7.rop_id
order by  avg_minutes_for_answer
