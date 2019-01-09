#!/bin/bash
tables='promotion sales_fact_1997 sales_fact_1998 sales_fact_dec_1998 store time_by_day'
for table in $tables
do
mysql -u user -ppassword --force <<EOF
use foodmart;
alter table $table add last_update INT;
update $table set last_update = UNIX_TIMESTAMP(NOW());
create trigger insert_$table before insert on $table for each row set new.last_update = unix_timestamp(now());
create trigger update_$table before update on $table for each row set new.last_update = unix_timestamp(now());
EOF
done
