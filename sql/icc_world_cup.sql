create database sandbox;
create schema praxis;

create table icc_world_cup
(
Team_1 Varchar(20),
Team_2 Varchar(20),
Winner Varchar(20)
);
INSERT INTO icc_world_cup values('India','SL','India');
INSERT INTO icc_world_cup values('SL','Aus','Aus');
INSERT INTO icc_world_cup values('SA','Eng','Eng');
INSERT INTO icc_world_cup values('Eng','NZ','NZ');
INSERT INTO icc_world_cup values('Aus','India','India');

select * from icc_world_cup;

with win_stats as (
select 
team_1 as team_name, case when team_1 = winner then 1 else 0 end as win_flag
from icc_world_cup
union all 
select team_2 as team_name, case when team_2 = winner then 1 else 0 end as win_flag
from icc_world_cup
)
select 
team_name, 
count(team_name) as total_matches_played, 
sum(win_flag) as matches_won, 
count(team_name) - sum(win_flag) as matches_loss
from win_stats
group by 1;