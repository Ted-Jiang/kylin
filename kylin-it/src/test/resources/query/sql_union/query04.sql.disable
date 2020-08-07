-- union subquery under join
select LSTG_FORMAT_NAME,
       SLR_SEGMENT_CD,
       CAL_DT,
       sum(CNT) as CNT
from
  (select LSTG_FORMAT_NAME,
          SLR_SEGMENT_CD,
          CAL_DT,
          sum(ITEM_COUNT) CNT
   from TEST_KYLIN_FACT
   where LSTG_FORMAT_NAME = 'ABIN'
   group by LSTG_FORMAT_NAME,
            SLR_SEGMENT_CD,
            CAL_DT
   UNION select 'NON-ABIN' as LSTG_FORMAT_NAME,
                    SLR_SEGMENT_CD,
                    CAL_DT,
                    case
                        when SLR_SEGMENT_CD > 1000 then CNT * 2
                        else CNT * 3
                    end as CNT
   from
     (select SLR_SEGMENT_CD,
             CAL_DT,
             sum(ITEM_COUNT) CNT
      from TEST_KYLIN_FACT
      where LSTG_FORMAT_NAME <> 'ABIN'
      group by SLR_SEGMENT_CD,CAL_DT))
group by LSTG_FORMAT_NAME,
         SLR_SEGMENT_CD,
         CAL_DT
order by CNT