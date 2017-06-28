#!/bin/bash

ANALY_DATE=$1
ANALY_HOUR=$2
DATABASE=$3

hive << EOF
use ${DATABASE};

alter table lte_mro_disturb_ana drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table lte_mro_disturb_ana add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table lte_mro_disturb_mix drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table lte_mro_disturb_mix add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table lte_mro_disturb_sec drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table lte_mro_disturb_sec add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table lte_mrs_dlbestrow_grid_ana60 drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table lte_mrs_dlbestrow_grid_ana60 add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table lte_mro_overlap_grid_ana60 drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table lte_mro_overlap_grid_ana60 add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table grid_ltemrkpi60 drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table grid_ltemrkpi60 add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table cell_ltemrkpitemp drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table cell_ltemrkpitemp add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table lte_mro_overlap_b_ana60 drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table lte_mro_overlap_b_ana60 add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table cell_ltenewmrkpi60 drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table cell_ltenewmrkpi60 add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table lte_mro_adjcover_ana60 drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table lte_mro_adjcover_ana60 add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table lte_mrs_overcover_ana60 drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table lte_mrs_overcover_ana60 add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table LTE_MRO_DISTURB_PRETREATE60 drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table LTE_MRO_DISTURB_PRETREATE60 add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table lte_mrs_dlbestrow_ana60 drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table lte_mrs_dlbestrow_ana60 add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});

alter table LTE_MRO_JOINUSER_ANA60 drop if exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
alter table LTE_MRO_JOINUSER_ANA60 add partition(dt=${ANALY_DATE},h=${ANALY_HOUR});
EOF
exit 0