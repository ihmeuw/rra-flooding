## Target file system
ROOT / 
    CaMa-Flood / cmf_v420_pkg /
    extracted_data / 
        {SOURCE} / 
            {INPUT_MEASURE}_{SCENARIO}_{MODEL}_{VARIANT}.nc
    cama_inputs /
        {MODEL}_{SCENARIO}_{VARIANT}_{BATCH} /
            run.sh
            runoff.ctl
            runoff /
                Roff____{YYYMMDD}.one
    cama_outputs / 
        {MODEL}_{SCENARIO}_{VARIANT}_{BATCH} /
            {VARIABLE}{YEAR}.bin
            ... crap
    results / 
        raw / 
            {SCENARIO} / 
                {VARIABLE} / 
                    {MODEL_VARIANT}.nc
        final /
            {SCENARIO} / 
                {VARIABLE_ADJUSTMENT_STATISTIC} / 
                    {MODEL_VARIANT}.nc
            
ROOT / 
    CaMa-Flood / cmf_v420_pkg /
    extracted_data / # /mnt/team/rapidresponse/pub/flooding/scratch/raw_data/esgf_metagrid
        {SOURCE} / 
            {INPUT_MEASURE}_{SCENARIO}_{MODEL}_{VARIANT}.nc
    cama_inputs /
        {MODEL}_{SCENARIO}_{VARIANT}_{BATCH} /
            run.sh              #/mnt/team/rapidresponse/pub/flooding/CaMa-Flood/cmf_v420_pkg/gosh
            runoff.ctl          # /mnt/team/rapidresponse/pub/flooding/CaMa-Flood/cmf_v420_pkg/inp
            runoff /            # /mnt/team/rapidresponse/pub/flooding/CaMa-Flood/cmf_v420_pkg/inp
                Roff____{YYYMMDD}.one
    cama_outputs / 
        {MODEL}_{SCENARIO}_{VARIANT}_{BATCH} /
            {OUTPUT_MEASURE}{YEAR}.bin
            ... crap
    results / 
        raw / 
            {SCENARIO} / 
                {VARIABLE} / 
                    {MODEL_VARIANT}.nc
        final /
            {SCENARIO} / 
                {VARIABLE_ADJUSTMENT_STATISTIC} / 
                    {MODEL_VARIANT}.nc