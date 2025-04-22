





#!/bin/bash


table_name=$1

bq query --nouse_legacy_sql --format=csv "SELECT ddl
FROM \`qlts-dev-mx-au-bro-verificacio.RTL_VERIFICACIONES.INFORMATION_SCHEMA.TABLES\`
WHERE table_name = '$table_name';" | tail -n +2 | tr -d '"'
