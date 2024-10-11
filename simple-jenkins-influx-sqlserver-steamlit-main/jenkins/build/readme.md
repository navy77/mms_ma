#### create table log
create table DATA_LOG_1 (
registered_at datetime,
status varchar(50),
process varchar(50),
message varchar(MAX),
error varchar(MAX)
)

create table DATA_LOG_2 (
registered_at datetime,
status varchar(50),
process varchar(50),
message varchar(MAX),
error varchar(MAX)
)

create table DATA_DEFECT (
registered_at datetime,
mc_no varchar(10),
process varchar(10),
d_str1 varchar(15),
d_str2 varchar(15),
emp_no float,
rssi float,
data varchar(100)
)

create table DATA_OUTPUT (
registered_at datetime,
mc_no varchar(10),
process varchar(10),
d_str1 varchar(15),
d_str2 varchar(15),
rssi float,
dmc_ok float,
)

create table MASTER_NG (
code float,
ng varchar(20),
)