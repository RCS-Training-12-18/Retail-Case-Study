#!/bin/bash
mysql -u user -ppassword --force <<EOF
use foodmart;
alter table promotion add last_update INT;
update promotion set last_update = UNIX_TIMESTAMP(NOW());
alter table sales_fact_1997 add last_update INT;
update table sales_fact_1997 set last_update = UNIX_TIMESTAMP(NOW());
alter table table sales_fact_1998 add last_update INT;
update table sales_fact_1998 set last_update = UNIX_TIMESTAMP(NOW());
alter table sales_fact_dec_1998 add last_update INT;
update sales_fact_dec_1998 set last_update = UNIX_TIMESTAMP(NOW());
alter table store add last_update INT;
update store set last_update = UNIX_TIMESTAMP(NOW());
alter table time_by_day add last_update INT;
update time_by_day set last_update = UNIX_TIMESTAMP(NOW());
create trigger sales_fact_1997_insert before insert on sales_fact_1997 for each row set new.last_update = unix_timestamp(now());
create trigger sales_fact_1997_update before update on sales_fact_1997 for each row set new.last_update = unix_timestamp(now());
create trigger sales_fact_1998_insert before insert on sales_fact_1998 for each row set new.last_update = unix_timestamp(now());
create trigger sales_fact_1998_update before update on sales_fact_1998 for each row set new.last_update = unix_timestamp(now());
create trigger sales_fact_dec_1998_insert before insert on sales_fact_dec_1998 for each row set new.last_update = unix_timestamp(now());
create trigger sales_fact_dec_1998_update before update on sales_fact_dec_1998 for each row set new.last_update = unix_timestamp(now());
create trigger promotion_insert before insert on promotion for each row set new.last_update = unix_timestamp(now());
create trigger promotion_update before update on promotion for each row set new.last_update = unix_timestamp(now());
create trigger store_insert before insert on store for each row set new.last_update = unix_timestamp(now());
create trigger store_update before update on store for each row set new.last_update = unix_timestamp(now());
create trigger time_by_day_insert before insert on time_by_day for each row set new.last_update = unix_timestamp(now());
create trigger time_by_day_update before update on time_by_day for each row set new.last_update = unix_timestamp(now());
EOF
clear
