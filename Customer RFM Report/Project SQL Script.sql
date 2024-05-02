-- Group By dữ liệu theo CustomerID để tính toán RFM
with group_by_tbl as (
	select
		T1.CustomerID,
		datediff('2022-09-01', MAX(T1.Purchase_Date)) as recency,
		count(distinct T1.Purchase_Date) / FLOOR(DATEDIFF('2022-09-01', T2.created_date) / 365.25) as frequency,
		SUM(GMV) / FLOOR(DATEDIFF(IFNULL(T2.stopdate, '2022-09-01'), T2.created_date) / 365.25) as monetary
	from customer_transaction T1
	left join customer_registered T2
	on T1.CustomerID = T2.ID
	where
		T1.CustomerID <> 0 and
		T2.ID is not NULL
	group by 
		T1.CustomerID
),
-- Chấm điểm RFM: trong khoảng 25% đầu tiên => 1, 50% => 2, 75% => 3, còn lại là 4
-- Chấm điểm recency
recency_tbl as (
	select
		*,
		case
			when rn_recency <= 0.2 * (select COUNT(*) from group_by_tbl) then 1
			when rn_recency <= 0.4 * (select COUNT(*) from group_by_tbl) then 2
			when rn_recency <= 0.6 * (select COUNT(*) from group_by_tbl) then 3
			when rn_recency <= 0.8 * (select COUNT(*) from group_by_tbl) then 4
			else 5
		end as recency_score
	from 
	(
		select *,
			row_number () OVER(order by recency DESC) as rn_recency
		from group_by_tbl
	) T1
),
frequency_tbl as (
	select
		*,
		case
			when rn_frequency <= 0.2 * (select COUNT(*) from group_by_tbl) then 1
			when rn_frequency <= 0.4 * (select COUNT(*) from group_by_tbl) then 2
			when rn_frequency <= 0.6 * (select COUNT(*) from group_by_tbl) then 3
			when rn_frequency <= 0.8 * (select COUNT(*) from group_by_tbl) then 4
			else 5
		end as frequency_score
	from 
	(
		select *,
			row_number () OVER(order by frequency) as rn_frequency
		from group_by_tbl
	) T1
),
monetary_tbl as (
	select
		*,
		case
			when rn_monetary <= 0.2 * (select COUNT(*) from group_by_tbl) then 1
			when rn_monetary <= 0.4 * (select COUNT(*) from group_by_tbl) then 2
			when rn_monetary <= 0.6 * (select COUNT(*) from group_by_tbl) then 3
			when rn_monetary <= 0.8 * (select COUNT(*) from group_by_tbl) then 4
			else 5
		end as monetary_score
	from 
	(
		select *,
			row_number () OVER(order by monetary) as rn_monetary
		from group_by_tbl
	) T1
),
RFM_tbl as (
	select
		T1.CustomerID,
		T1.recency_score,
		T2.frequency_score,
		T3.monetary_score,
		CONCAT(T1.recency_score, T2.frequency_score, T3.monetary_score) as RFM_score
	from recency_tbl T1
	join frequency_tbl T2
	on T1.CustomerID = T2.CustomerID
	join monetary_tbl T3
	on T1.CustomerID = T3.CustomerID
),
Result_tbl as (
	select 
		RFM_Score,
		COUNT(distinct CustomerID) as Total_Customers
	from RFM_tbl
	group by
		RFM_Score
	order by 
		2 DESC
)

select *
from Result_tbl