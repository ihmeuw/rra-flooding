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
            {OUTPUT_MEASURE}{YEAR}.bin
            ... crap
    results / 
        raw / 
            {SCENARIO} / 
                {MEASURE} / 
                    {MODEL_VARIANT}.nc
        final /
            {SCENARIO} / 
                {FINAL_MEASURE} / 
                    {MODEL_VARIANT}.nc
            
